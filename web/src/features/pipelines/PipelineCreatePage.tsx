import { useHintFeedback } from '@/components/hintFeedback'
import { HintForm } from '@/components/HintForm'
import { HintIcon } from '@/components/HintIcon'
import { useEffect, useRef, useState } from 'react'
import {
  Button,
  Card,
  Checkbox,
  DatePicker,
  Descriptions,
  Form,
  Input,
  InputNumber,
  Select,
  Space,
  Steps,
  Typography,
} from 'antd'
import { useTranslation } from 'react-i18next'
import { useNavigate } from 'react-router-dom'
import type { Dayjs } from 'dayjs'
import {
  useCreatePipelineMutation,
  useStartPipelineMutation,
  type PipelineCreateInput,
  type PipelineInitMode,
} from '@/api/pipelines'
import { useEndpointsQuery } from '@/api/endpoints'
import { useNodesQuery } from '@/api/nodes'
import { createDefaultStrategyRules, type StrategyRules } from '@/api/strategies'
import { fromWireRules, toWireRules } from '@/api/strategyRulesWire'
import { StrategyRuleEditorDrawer } from '@/features/strategies/StrategyRuleEditorDrawer'
import { runPipelinePrecheck } from '@/api/precheck'
import { isApiMockEnabled } from '@/mocks/config'
import { useCurrentUser } from '@/api/auth'
import { useCreatePipelinePairMutation, type PipelinePairInput } from '@/api/pipelinePairs'
import {
  preparePipelinePairDraft,
  readPipelinePairDraft,
  storePipelinePairDraft,
} from './pipelinePairDraft'

// 节点/端点候选列表用固定上限一次性取全量做本地下拉，容量对齐服务端
// `maxListPageSize`（见 internal/manager/http/pagination.go）——这不是"拉
// 全量"，只是把查找表的上限设到服务端本来就允许的单页最大值，与
// web/src/features/pipelines/PipelinesPage.tsx 的 LOOKUP_PAGE_SIZE 同一先例。
const LOOKUP_PAGE_SIZE = 200

// Keep Select menus in their local containers so overlapping option labels can be queried independently.
function popupContainer(trigger: HTMLElement): HTMLElement {
  return (trigger.parentElement as HTMLElement | null) ?? document.body
}

const INIT_MODES: PipelineInitMode[] = [
  'full_and_incremental',
  'incremental_only_latest',
  'incremental_only_gtid_set',
  'incremental_only_binlog_pos',
  'incremental_only_timestamp',
  'full_only',
  'schema_only',
]

type PositionMode = 'gtid_set' | 'binlog_pos' | 'timestamp' | 'none'

function positionModeFor(initMode: PipelineInitMode | undefined): PositionMode {
  switch (initMode) {
    case 'incremental_only_gtid_set':
      return 'gtid_set'
    case 'incremental_only_binlog_pos':
      return 'binlog_pos'
    case 'incremental_only_timestamp':
      return 'timestamp'
    default:
      return 'none'
  }
}

interface WizardFormValues {
  name: string
  sourceEndpointId: string
  sourceNodeId: string
  targetEndpointId: string
  targetNodeId: string
  initMode: PipelineInitMode
  gtidSet?: string
  binlogFile?: string
  binlogPos?: number
  timestamp?: Dayjs
}

function positionFieldNames(mode: PositionMode): (keyof WizardFormValues)[] {
  switch (mode) {
    case 'gtid_set':
      return ['gtidSet']
    case 'binlog_pos':
      return ['binlogFile', 'binlogPos']
    case 'timestamp':
      return ['timestamp']
    default:
      return []
  }
}

/** 仅创建时可填，结构随 initMode 变化，见 api/pipelines.ts 文件头注释；
 * 不需要位点的初始化方式不提交这个字段（undefined，不是空对象）。 */
function buildStartPosition(initMode: PipelineInitMode, values: WizardFormValues): unknown {
  switch (positionModeFor(initMode)) {
    case 'gtid_set':
      return { gtidSet: values.gtidSet }
    case 'binlog_pos':
      return { source_file: values.binlogFile, source_offset: values.binlogPos }
    case 'timestamp':
      return { source_ts_ms: values.timestamp?.valueOf() }
    default:
      return undefined
  }
}

export default function PipelineCreatePage() {
  const { t } = useTranslation()
  const { message } = useHintFeedback()
  const navigate = useNavigate()
  const [form] = Form.useForm<WizardFormValues>()
  const [current, setCurrent] = useState(0)
  const [ruleEditorOpen, setRuleEditorOpen] = useState(false)
  const [rules, setRules] = useState<StrategyRules>(createDefaultStrategyRules)
  const [reverseRules, setReverseRules] = useState<StrategyRules>(
    createDefaultStrategyRules,
  )
  const [createReverse, setCreateReverse] = useState(false)
  const [editingReverse, setEditingReverse] = useState(false)
  const { user } = useCurrentUser()
  const pairMutation = useCreatePipelinePairMutation()
  const pendingPair = useRef<PipelinePairInput | undefined>(undefined)
  const rulesEdited = useRef(false)
  const [phase, setPhase] = useState<'idle' | 'submitting'>('idle')

  const endpointsQuery = useEndpointsQuery({ page: 1, pageSize: LOOKUP_PAGE_SIZE })

  const sourceEndpointId = Form.useWatch('sourceEndpointId', form)
  const targetEndpointId = Form.useWatch('targetEndpointId', form)
  const initMode = Form.useWatch('initMode', form)

  const endpointById = new Map((endpointsQuery.data?.items ?? []).map((e) => [e.id, e]))

  const sourceCenterId = sourceEndpointId
    ? endpointById.get(sourceEndpointId)?.centerId
    : undefined
  const targetCenterId = targetEndpointId
    ? endpointById.get(targetEndpointId)?.centerId
    : undefined

  // C-19/C-16（internal/manager/http/pipelines.go validatePipelineEndpoints）：
  // 源节点必须与源端点同中心，目标节点必须与目标端点同中心——这是两条独立
  // 约束（源和目标彼此可以不在同一中心，跨中心正是这套系统存在的意义），
  // 不是"源和目标必须同中心"。这里按各自的 centerId 独立过滤节点候选。
  const sourceNodesQuery = useNodesQuery(
    { page: 1, pageSize: LOOKUP_PAGE_SIZE, centerId: sourceCenterId },
    Boolean(sourceCenterId),
  )
  const targetNodesQuery = useNodesQuery(
    { page: 1, pageSize: LOOKUP_PAGE_SIZE, centerId: targetCenterId },
    Boolean(targetCenterId),
  )

  const createMutation = useCreatePipelineMutation()
  const startMutation = useStartPipelineMutation()

  const positionMode = positionModeFor(initMode)
  const [restoredUser, setRestoredUser] = useState<string>()
  const [restoredDraft, setRestoredDraft] = useState<PipelinePairInput>()
  if (user?.id && restoredUser !== user.id) {
    const pending = readPipelinePairDraft(user.id)
    setRestoredDraft(pending)
    setRestoredUser(user.id)
    if (pending) {
      setCreateReverse(true)
      setRules(
        pending.a.rules
          ? fromWireRules(pending.a.rules, createDefaultStrategyRules()).rules
          : createDefaultStrategyRules(),
      )
      setReverseRules(
        pending.b.rules
          ? fromWireRules(pending.b.rules, createDefaultStrategyRules()).rules
          : createDefaultStrategyRules(),
      )
    }
  }
  useEffect(() => {
    const pending = restoredDraft
    pendingPair.current = pending
    if (!pending) return
    form.setFieldsValue({
      name: pending.name,
      sourceEndpointId: pending.a.endpointId,
      targetEndpointId: pending.b.endpointId,
      initMode: 'full_and_incremental',
    })
  }, [restoredDraft, form])

  // Clear incompatible node selections when endpoint centers change, including programmatic form updates.
  useEffect(() => {
    const pending = pendingPair.current
    form.setFieldValue(
      'sourceNodeId',
      pending && pending.a.endpointId === sourceEndpointId ? pending.a.nodeId : undefined,
    )
  }, [form, sourceEndpointId])
  useEffect(() => {
    const pending = pendingPair.current
    form.setFieldValue(
      'targetNodeId',
      pending && pending.b.endpointId === targetEndpointId ? pending.b.nodeId : undefined,
    )
  }, [form, targetEndpointId])
  useEffect(() => {
    form.setFieldsValue({
      gtidSet: undefined,
      binlogFile: undefined,
      binlogPos: undefined,
      timestamp: undefined,
    })
  }, [form, initMode])

  const steps = [
    t('pipelines.wizard.steps.basics'),
    t('pipelines.wizard.steps.endpoints'),
    t('pipelines.wizard.steps.strategy'),
    t('pipelines.wizard.steps.initMode'),
    t('pipelines.wizard.steps.precheck'),
    t('pipelines.wizard.steps.review'),
  ]

  const stepFieldNames: (keyof WizardFormValues)[][] = [
    ['name'],
    ['sourceEndpointId', 'sourceNodeId', 'targetEndpointId', 'targetNodeId'],
    [],
    ['initMode', ...positionFieldNames(positionMode)],
    [],
  ]

  async function goNext() {
    try {
      await form.validateFields(stepFieldNames[current])
    } catch {
      return
    }
    setCurrent((c) => c + 1)
  }

  function goPrev() {
    setCurrent((c) => c - 1)
  }

  async function handleSubmit() {
    let values: WizardFormValues
    try {
      values = (await form.validateFields([
        'name',
        'sourceEndpointId',
        'sourceNodeId',
        'targetEndpointId',
        'targetNodeId',
        'initMode',
        ...positionFieldNames(positionMode),
      ])) as WizardFormValues
    } catch {
      return
    }

    const mapped = toWireRules(rules)
    if (!mapped.ok) {
      void message.error(t('pipelines.rules.hint'))
      return
    }
    setPhase('submitting')
    if (createReverse) {
      const mappedReverse = toWireRules(reverseRules)
      if (!mappedReverse.ok) {
        setPhase('idle')
        void message.error(t('pipelines.rules.hint'))
        return
      }
      const input = preparePipelinePairDraft(
        {
          name: values.name.trim(),
          a: {
            endpointId: values.sourceEndpointId,
            nodeId: values.sourceNodeId,
            rules: mapped.wire,
          },
          b: {
            endpointId: values.targetEndpointId,
            nodeId: values.targetNodeId,
            rules: mappedReverse.wire,
          },
        },
        pendingPair.current,
        rulesEdited.current,
      )
      pendingPair.current = input
      if (user?.id) storePipelinePairDraft(user.id, input)
      try {
        await pairMutation.mutateAsync(input)
        pendingPair.current = undefined
        if (user?.id) storePipelinePairDraft(user.id, undefined)
        void message.success(t('pipelines.pair.success'))
        navigate('/pipelines')
      } catch (error) {
        setPhase('idle')
        void message.error(
          t('pipelines.wizard.createFailed', { reason: (error as Error).message }),
        )
      }
      return
    }
    const payload: PipelineCreateInput = {
      name: values.name,
      rules: mapped.wire,
      sourceEndpointId: values.sourceEndpointId,
      sourceNodeId: values.sourceNodeId,
      targetEndpointId: values.targetEndpointId,
      targetNodeId: values.targetNodeId,
      initMode: values.initMode,
      startPosition: buildStartPosition(values.initMode, values),
    }

    let created
    try {
      created = await createMutation.mutateAsync(payload)
    } catch (error) {
      setPhase('idle')
      void message.error(
        t('pipelines.wizard.createFailed', { reason: (error as Error).message }),
      )
      return
    }

    if (values.initMode === 'incremental_only_timestamp') {
      navigate(`/pipelines/${created.id}`)
      return
    }
    if (
      !isApiMockEnabled() &&
      (values.initMode === 'incremental_only_gtid_set' ||
        values.initMode === 'incremental_only_binlog_pos')
    ) {
      navigate(`/pipelines/${created.id}#position`, {
        state: { startFailedNotice: t('pipelines.position.createdNeedsPreview') },
      })
      return
    }
    try {
      if (!isApiMockEnabled()) {
        const precheck = await runPipelinePrecheck(created.id)
        if (!precheck.canStart) {
          navigate(`/pipelines/${created.id}#precheck`, {
            state: { startFailedNotice: t('pipelines.precheck.createdNeedsCheck') },
          })
          return
        }
      }
      await startMutation.mutateAsync(created.id)
      void message.success(t('pipelines.wizard.createdAndStarted', { name: created.name }))
      navigate(`/pipelines/${created.id}`)
    } catch (error) {
      navigate(`/pipelines/${created.id}`, {
        state: {
          startFailedNotice: t('pipelines.wizard.startFailedNotice', {
            reason: (error as Error).message,
          }),
        },
      })
    }
  }

  return (
    <div>
      <Typography.Title level={4}>{t('pipelines.wizard.title')}</Typography.Title>
      <Steps
        current={current}
        items={steps.map((title) => ({ title }))}
        style={{ marginBottom: 16 }}
      />

      <Form<WizardFormValues>
        form={form}
        name="pipelineCreateWizard"
        layout="vertical"
        initialValues={{ initMode: 'full_and_incremental' }}
      >
        <div
          data-testid="wizard-step-basics"
          style={{ display: current === 0 ? 'block' : 'none' }}
        >
          <Checkbox
            checked={createReverse}
            onChange={(event) => {
              setCreateReverse(event.target.checked)
              if (event.target.checked)
                form.setFieldValue('initMode', 'full_and_incremental')
            }}
            style={{ marginBottom: 16 }}
          >
            {t('pipelines.wizard.createReverse')}
          </Checkbox>
          <HintForm
            name="name"
            label={t('pipelines.wizard.name')}
            rules={[
              {
                required: true,
                whitespace: true,
                message: t('pipelines.wizard.nameRequired'),
              },
              { max: 128 },
            ]}
          >
            <Input maxLength={128} />
          </HintForm>
        </div>

        <div
          data-testid="wizard-step-endpoints"
          className="compact-card-grid"
          style={{ display: current === 1 ? 'grid' : 'none' }}
        >
          <Card
            size="small"
            title={t('pipelines.wizard.sourceTitle')}
            data-testid="wizard-source-card"
          >
            <HintForm
              name="sourceEndpointId"
              label={t('pipelines.wizard.endpoint')}
              rules={[{ required: true, message: t('pipelines.wizard.endpointRequired') }]}
            >
              <Select
                showSearch
                getPopupContainer={popupContainer}
                loading={endpointsQuery.isLoading}
                placeholder={t('pipelines.wizard.endpointPlaceholder')}
                filterOption={(input, option) =>
                  (option?.label ?? '')
                    .toString()
                    .toLowerCase()
                    .includes(input.toLowerCase())
                }
                options={(endpointsQuery.data?.items ?? []).map((e) => ({
                  value: e.id,
                  label: e.name,
                }))}
              />
            </HintForm>
            <HintForm
              name="sourceNodeId"
              label={t('pipelines.wizard.node')}
              extra={t('pipelines.wizard.nodeCenterHint')}
              rules={[{ required: true, message: t('pipelines.wizard.nodeRequired') }]}
            >
              <Select
                showSearch
                getPopupContainer={popupContainer}
                disabled={!sourceCenterId}
                loading={sourceNodesQuery.isLoading}
                placeholder={
                  sourceCenterId
                    ? undefined
                    : t('pipelines.wizard.nodePlaceholderBeforeEndpoint')
                }
                notFoundContent={
                  sourceCenterId ? t('pipelines.wizard.nodeEmptyForCenter') : undefined
                }
                filterOption={(input, option) =>
                  (option?.label ?? '')
                    .toString()
                    .toLowerCase()
                    .includes(input.toLowerCase())
                }
                options={(sourceNodesQuery.data?.items ?? []).map((n) => ({
                  value: n.id,
                  label: n.name,
                }))}
              />
            </HintForm>
          </Card>

          <Card
            size="small"
            title={t('pipelines.wizard.targetTitle')}
            data-testid="wizard-target-card"
          >
            <HintForm
              name="targetEndpointId"
              label={t('pipelines.wizard.endpoint')}
              rules={[{ required: true, message: t('pipelines.wizard.endpointRequired') }]}
            >
              <Select
                showSearch
                getPopupContainer={popupContainer}
                loading={endpointsQuery.isLoading}
                placeholder={t('pipelines.wizard.endpointPlaceholder')}
                filterOption={(input, option) =>
                  (option?.label ?? '')
                    .toString()
                    .toLowerCase()
                    .includes(input.toLowerCase())
                }
                options={(endpointsQuery.data?.items ?? []).map((e) => ({
                  value: e.id,
                  label: e.name,
                }))}
              />
            </HintForm>
            <HintForm
              name="targetNodeId"
              label={t('pipelines.wizard.node')}
              extra={t('pipelines.wizard.nodeCenterHint')}
              rules={[{ required: true, message: t('pipelines.wizard.nodeRequired') }]}
            >
              <Select
                showSearch
                getPopupContainer={popupContainer}
                disabled={!targetCenterId}
                loading={targetNodesQuery.isLoading}
                placeholder={
                  targetCenterId
                    ? undefined
                    : t('pipelines.wizard.nodePlaceholderBeforeEndpoint')
                }
                notFoundContent={
                  targetCenterId ? t('pipelines.wizard.nodeEmptyForCenter') : undefined
                }
                filterOption={(input, option) =>
                  (option?.label ?? '')
                    .toString()
                    .toLowerCase()
                    .includes(input.toLowerCase())
                }
                options={(targetNodesQuery.data?.items ?? []).map((n) => ({
                  value: n.id,
                  label: n.name,
                }))}
              />
            </HintForm>
          </Card>
        </div>

        <div
          data-testid="wizard-step-strategy"
          style={{ display: current === 2 ? 'block' : 'none' }}
        >
          <Typography.Paragraph>{t('pipelines.rules.default')}</Typography.Paragraph>
          <Button
            onClick={() => {
              setEditingReverse(false)
              setRuleEditorOpen(true)
            }}
          >
            {t('pipelines.rules.configure')}
          </Button>
          {createReverse && (
            <Button
              style={{ marginLeft: 8 }}
              onClick={() => {
                setEditingReverse(true)
                setRuleEditorOpen(true)
              }}
            >
              {t('pipelines.wizard.configureReverseRules')}
            </Button>
          )}
          <pre style={{ maxHeight: 300, overflow: 'auto', marginTop: 16 }}>
            {JSON.stringify(rules, null, 2)}
          </pre>
        </div>

        <div
          data-testid="wizard-step-init-mode"
          style={{ display: current === 3 ? 'block' : 'none' }}
        >
          <HintForm
            name="initMode"
            label={t('pipelines.wizard.steps.initMode')}
            extra={
              <>
                {t('pipelines.wizard.startPositionManualHint')}
                {positionMode === 'none' && t('pipelines.wizard.startPositionNotNeeded')}
                {createReverse && (
                  <>
                    {t('pipelines.pair.initialization')}
                    {t('pipelines.pair.isolation')}
                  </>
                )}
              </>
            }
            rules={[{ required: true }]}
          >
            <Select
              disabled={createReverse}
              getPopupContainer={popupContainer}
              options={INIT_MODES.map((mode) => ({
                value: mode,
                label: t(`pipelines.initMode.${mode}`),
              }))}
            />
          </HintForm>
          {positionMode === 'gtid_set' && (
            <HintForm
              name="gtidSet"
              label={t('pipelines.wizard.startPositionGtidSet')}
              rules={[
                {
                  required: true,
                  message: t('pipelines.wizard.startPositionGtidSetRequired'),
                },
              ]}
            >
              <Input placeholder={t('pipelines.wizard.startPositionGtidSetPlaceholder')} />
            </HintForm>
          )}
          {positionMode === 'binlog_pos' && (
            <Space.Compact block>
              <HintForm
                name="binlogFile"
                label={t('pipelines.wizard.startPositionFile')}
                style={{ width: '60%' }}
                rules={[
                  {
                    required: true,
                    message: t('pipelines.wizard.startPositionFileRequired'),
                  },
                ]}
              >
                <Input placeholder={t('pipelines.wizard.startPositionFilePlaceholder')} />
              </HintForm>
              <HintForm
                name="binlogPos"
                label={t('pipelines.wizard.startPositionPos')}
                style={{ width: '40%' }}
                rules={[
                  {
                    required: true,
                    message: t('pipelines.wizard.startPositionPosRequired'),
                  },
                ]}
              >
                <InputNumber min={0} style={{ width: '100%' }} />
              </HintForm>
            </Space.Compact>
          )}
          {positionMode === 'timestamp' && (
            <HintForm
              name="timestamp"
              label={t('pipelines.wizard.startPositionTimestamp')}
              rules={[
                {
                  required: true,
                  message: t('pipelines.wizard.startPositionTimestampRequired'),
                },
              ]}
            >
              <DatePicker showTime style={{ width: '100%' }} />
            </HintForm>
          )}
        </div>

        <div style={{ display: current === 4 ? 'block' : 'none' }}>
          {/*
            执行接口需要已创建的 pipeline ID 与当前配置版本。
            这里说明检查时机，提交后执行真实检查；警告确认及证据查看由
            详情页承载，不能用向导本地校验生成启动授权。
          */}
          <Typography.Title level={5}>
            <span className="section-title">
              {t('pipelines.wizard.precheckPlaceholderTitle')}
              <HintIcon
                content={
                  <>
                    {t(
                      createReverse
                        ? 'pipelines.wizard.pairPrecheck'
                        : 'pipelines.wizard.precheckPlaceholderDesc',
                    )}
                  </>
                }
              />
            </span>
          </Typography.Title>
        </div>

        {current === 5 && (
          <Card size="small" title={t('pipelines.wizard.reviewTitle')}>
            <Descriptions column={2} size="small">
              <Descriptions.Item label={t('pipelines.wizard.name')}>
                {form.getFieldValue('name')}
              </Descriptions.Item>

              <Descriptions.Item label={t('pipelines.wizard.direction')}>
                {t(
                  createReverse
                    ? 'pipelines.wizard.bidirectional'
                    : 'pipelines.wizard.oneWay',
                )}
              </Descriptions.Item>

              <Descriptions.Item label={t('pipelines.detail.sourceEndpoint')}>
                {(sourceEndpointId && endpointById.get(sourceEndpointId)?.name) ??
                  sourceEndpointId}
              </Descriptions.Item>
              <Descriptions.Item label={t('pipelines.detail.targetEndpoint')}>
                {(targetEndpointId && endpointById.get(targetEndpointId)?.name) ??
                  targetEndpointId}
              </Descriptions.Item>
              <Descriptions.Item label={t('pipelines.detail.sourceNode')}>
                {(sourceNodesQuery.data?.items ?? []).find(
                  (n) => n.id === form.getFieldValue('sourceNodeId'),
                )?.name ?? form.getFieldValue('sourceNodeId')}
              </Descriptions.Item>
              <Descriptions.Item label={t('pipelines.detail.targetNode')}>
                {(targetNodesQuery.data?.items ?? []).find(
                  (n) => n.id === form.getFieldValue('targetNodeId'),
                )?.name ?? form.getFieldValue('targetNodeId')}
              </Descriptions.Item>
              <Descriptions.Item label={t('pipelines.detail.initMode')} span={2}>
                {t(`pipelines.initMode.${initMode}`)}
              </Descriptions.Item>
              <Descriptions.Item label={t('pipelines.detail.startPosition')} span={2}>
                {positionMode === 'none' ? (
                  t('pipelines.wizard.startPositionNotNeeded')
                ) : (
                  <Typography.Text code>
                    {JSON.stringify(
                      buildStartPosition(
                        initMode,
                        form.getFieldsValue(true) as WizardFormValues,
                      ),
                    )}
                  </Typography.Text>
                )}
              </Descriptions.Item>
            </Descriptions>
          </Card>
        )}
      </Form>

      <Space style={{ marginTop: 24 }}>
        {current === 0 ? (
          <Button onClick={() => navigate('/pipelines')}>
            {t('pipelines.wizard.back')}
          </Button>
        ) : (
          <Button onClick={goPrev} disabled={phase === 'submitting'}>
            {t('pipelines.wizard.prev')}
          </Button>
        )}
        {current < steps.length - 1 ? (
          <Button type="primary" onClick={() => void goNext()}>
            {t('pipelines.wizard.next')}
          </Button>
        ) : (
          <Button
            type="primary"
            loading={phase === 'submitting'}
            onClick={() => void handleSubmit()}
          >
            {phase === 'submitting'
              ? t('pipelines.wizard.submitting')
              : t(
                  createReverse ? 'pipelines.wizard.submitPair' : 'pipelines.wizard.submit',
                )}
          </Button>
        )}
      </Space>

      <StrategyRuleEditorDrawer
        open={ruleEditorOpen}
        onClose={() => setRuleEditorOpen(false)}
        initMode={editingReverse ? 'incremental_only_latest' : initMode}
        baseRules={editingReverse ? reverseRules : rules}
        baseNotices={[]}
        onSave={async (next) => {
          rulesEdited.current = true
          if (editingReverse) setReverseRules(next)
          else setRules(next)
        }}
      />
    </div>
  )
}
