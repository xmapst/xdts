import { LuaRowFilterFields } from './LuaRowFilterFields'
import { ActionDrawer } from '@/components/ActionDrawer'
import { useHintFeedback } from '@/components/hintFeedback'
import { HintAlert as Alert } from '@/components/HintAlert'
import { HintForm } from '@/components/HintForm'
import { HintIcon } from '@/components/HintIcon'
import { useEffect, useState } from 'react'
import { Button, Checkbox, Collapse, Form, Input, InputNumber, Select, Space, Tabs } from 'antd'
import { MinusCircleOutlined, PlusOutlined } from '@ant-design/icons'
import { useTranslation } from 'react-i18next'
import type { ObjectFilterRule, StrategyRules } from '@/api/strategies'
import type { PipelineInitMode } from '@/api/pipelines'
import { useCreateStrategyVersionMutation } from '@/api/strategies'
import { validateRules } from './validateRules'
import type { WireNotice } from '@/api/strategyRulesWire'
import { TransformRuleFields } from './TransformRuleFields'
import { MappingRuleFields } from './MappingRuleFields'
import { useEndpointsQuery } from '@/api/endpoints'

/** 「添加规则」按钮的初始值：action 默认放行、schema 默认精确匹配、库/表
 * 模式都留空等待用户填写——与 createDefaultStrategyRules() 的"空数组=放行
 * 全部"默认值不冲突，这只是用户主动点了"添加规则"之后单条规则的起始值。 */
function createEmptyObjectFilterRule(): ObjectFilterRule {
  return {
    action: 'FILTER_ACTION_INCLUDE',
    schema_match: 'MATCH_TYPE_EXACT',
    schema_pattern: '',
    table_match: 'MATCH_TYPE_UNSPECIFIED',
    table_pattern: '',
  }
}

interface StrategyRuleEditorDrawerProps {
  open: boolean
  onClose: () => void
  strategyId?: string
  onSave?: (rules: StrategyRules) => Promise<void>
  saving?: boolean
  initMode?: PipelineInitMode

  baseRules: StrategyRules
  /* 入站映射产出的"本界面表达不出"清单（`fromWireRules` 的 notices）。 */
  baseNotices: WireNotice[]
}

/**
 * 规则编辑弹窗：提交即创建新版本（MGR-STR-01，版本不可变）。表单编辑与 JSON
 * 编辑保持同一份 `rules` 状态作为唯一真源，切换 Tab 时做一次显式同步——
 * 从 JSON 切到表单时解析并回填表单，从表单切到 JSON 时把当前表单值序列化
 * 展示；不在每次按键时都互相同步，避免输入 JSON 时光标跳动、也避免两边
 * 同时监听造成循环更新。P1/P2 类别（表单未覆盖的字段）保留在 `advanced`
 * 里，只能通过 JSON 编辑，表单切换不会把它冲掉。
 */
function mergeFormRules(rules: StrategyRules, values: StrategyRules): StrategyRules {
  const window = values.incremental_window
  const configured = window && Object.values(window).some((value) => value != null)
  return {
    ...rules,
    ...values,
    incremental_window: configured
      ? {
          max_batches: window.max_batches ?? 1,
          max_bytes: window.max_bytes ?? 67108864,
          reorder_timeout_ms: window.reorder_timeout_ms ?? 60000,
        }
      : undefined,
  }
}

export function StrategyRuleEditorDrawer({
  open,
  onClose,
  strategyId,
  onSave,
  saving = false,
  baseRules,
  baseNotices,
  initMode,
}: StrategyRuleEditorDrawerProps) {
  const { t } = useTranslation()
  const { message } = useHintFeedback()
  const [form] = Form.useForm<StrategyRules>()
  const createVersionMutation = useCreateStrategyVersionMutation(strategyId ?? '')

  const [rules, setRules] = useState<StrategyRules>(baseRules)
  const [activeTab, setActiveTab] = useState<'form' | 'json'>('form')
  const [jsonText, setJsonText] = useState('')
  const [jsonError, setJsonError] = useState<string | null>(null)
  const [validationErrors, setValidationErrors] = useState<string[]>([])

  useEffect(() => {
    if (!open) {
      return
    }
    setRules(baseRules)
    form.setFieldsValue(baseRules)
    setJsonText(JSON.stringify(baseRules, null, 2))
    setJsonError(null)
    setValidationErrors([])
    setActiveTab('form')
    // baseRules 只在弹窗刚打开那一刻取一次快照，后续表单/JSON 内部的编辑
    // 不应该被外部 baseRules 引用变化影响，因此只依赖 open。
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open])

  const handleTabChange = (nextTab: string) => {
    if (activeTab === 'json' && nextTab === 'form') {
      try {
        const parsed = JSON.parse(jsonText) as StrategyRules
        form.setFieldsValue(parsed)
        setRules(parsed)
        setJsonError(null)
      } catch {
        setJsonError(t('strategies.editor.jsonInvalid'))
        return
      }
    } else if (activeTab === 'form' && nextTab === 'json') {
      const values = form.getFieldsValue(true) as StrategyRules
      const merged = mergeFormRules(rules, values)
      setRules(merged)
      setJsonText(JSON.stringify(merged, null, 2))
    }
    setActiveTab(nextTab as 'form' | 'json')
  }

  const handleSubmit = () => {
    // 提交前统一走一次「表单 → rules」的同步（用户可能停在表单 tab 直接点提交，
    // 没有经过 tab 切换那次同步）。
    let finalRules = rules
    if (activeTab === 'form') {
      const values = form.getFieldsValue(true) as StrategyRules
      finalRules = mergeFormRules(rules, values)
    } else {
      try {
        finalRules = JSON.parse(jsonText) as StrategyRules
      } catch {
        setJsonError(t('strategies.editor.jsonInvalid'))
        return
      }
    }

    // 有 unrepresentable 的告知项时**直接不提交**。放在业务校验之前，是因为
    // 它与用户这次填了什么无关——先让人修一堆字段、最后再说"其实这个区块根本
    // 不能提交"是更糟的顺序。
    if (baseNotices.some((n) => n.kind === 'unrepresentable')) {
      setValidationErrors([t('strategies.editor.notices.blockedSubmit')])
      return
    }

    const errors = validateRules(finalRules)
    if (errors.length > 0) {
      setValidationErrors(errors.map((e) => e.message))
      return
    }
    setValidationErrors([])

    if (onSave) {
      void onSave(finalRules)
        .then(onClose)
        .catch((error: Error) => void message.error(error.message))
      return
    }
    createVersionMutation.mutate(finalRules, {
      onSuccess: () => {
        void message.success(t('common.operationSucceeded'))
        onClose()
      },
      onError: (error) => void message.error(error.message),
    })
  }

  return (
    <ActionDrawer
      open={open}
      title={
        <span className="section-title">
          {t('strategies.editor.title')}
          <HintIcon
            content={t(onSave ? 'pipelines.rules.hint' : 'strategies.versions.createVersionHint')}
          />
        </span>
      }
      onClose={onClose}
      onConfirm={handleSubmit}
      confirmLoading={saving || createVersionMutation.isPending}
      confirmText={t('common.submit')}
      cancelText={t('common.cancel')}
      size={960}
      destroyOnHidden
    >
      <Tabs
        tabBarExtraContent={
          <span className="section-title">
            {baseNotices.length > 0 && (
              <Alert
                type="warning"
                showIcon
                style={{ marginBottom: 0 }}
                title={t('strategies.editor.notices.title')}
                description={
                  <ul style={{ margin: 0, paddingLeft: 20 }}>
                    {baseNotices.map((n, i) => (
                      <li key={`${n.code}-${i}`}>
                        {n.code === 'integerUnrepresentable' &&
                          t('strategies.editor.notices.integerUnrepresentable', {
                            field: n.params.field,
                            value: JSON.stringify(n.params.value),
                          })}
                        {n.code === 'replicaEndpointUnrepresentable' &&
                          t('strategies.editor.notices.replicaEndpointUnrepresentable', {
                            value: JSON.stringify(n.params.value),
                          })}
                        {n.code === 'ddlDangerousMismatch' &&
                          t('strategies.editor.notices.ddlDangerousMismatch', {
                            allowDrop: String(n.params.allowDrop),
                            allowTruncate: String(n.params.allowTruncate),
                            willSetBothTo: String(n.params.willSetBothTo),
                          })}
                        {n.code === 'enumUnrecognized' &&
                          t('strategies.editor.notices.enumUnrecognized', {
                            field: String(n.params.field),
                            value: String(n.params.value),
                            fallback: String(n.params.fallback),
                          })}
                        {n.code === 'lwwTablesUnrepresentable' && (
                          <>
                            {t('strategies.editor.notices.lwwTablesUnrepresentable')}
                            {}
                            <pre
                              style={{
                                margin: '4px 0 0',
                                fontSize: 12,
                                whiteSpace: 'pre-wrap',
                              }}
                            >
                              {JSON.stringify(n.params.tables, null, 2)}
                            </pre>
                          </>
                        )}
                      </li>
                    ))}
                  </ul>
                }
              />
            )}

            {jsonError && <HintIcon type="error" content={jsonError} />}
            {validationErrors.length > 0 && (
              <HintIcon
                type="error"
                content={
                  <>
                    {t('strategies.editor.validationErrors')}
                    <ul style={{ margin: '4px 0 0', paddingLeft: 20 }}>
                      {validationErrors.map((msg) => (
                        <li key={msg}>{msg}</li>
                      ))}
                    </ul>
                  </>
                }
              />
            )}
          </span>
        }
        activeKey={activeTab}
        onChange={handleTabChange}
        items={[
          {
            key: 'form',
            label: t('strategies.editor.formTab'),
            children: (
              <Form<StrategyRules> form={form} layout="vertical" initialValues={rules}>
                <RuleFormSections open={open} initMode={initMode} />
              </Form>
            ),
          },
          {
            key: 'json',
            label: t('strategies.editor.jsonTab'),
            children: (
              <>
                <Input.TextArea
                  value={jsonText}
                  onChange={(event) => {
                    setJsonText(event.target.value)
                    setJsonError(null)
                  }}
                  rows={18}
                  style={{ fontFamily: 'monospace', fontSize: 12 }}
                />
              </>
            ),
          },
        ]}
      />
    </ActionDrawer>
  )
}

/** 规则分类的表单区块。 */
function RuleFormSections({ open, initMode }: { open: boolean; initMode?: PipelineInitMode }) {
  const { t } = useTranslation()
  const form = Form.useFormInstance<StrategyRules>()
  const replicaEnabled = Form.useWatch(['snapshotParams', 'readFromReplica'], form)
  const originMode = Form.useWatch(['origin_policy', 'mode'], { form, preserve: true })
  const ddlMode = Form.useWatch(['ddlPolicy', 'mode'], { form, preserve: true })
  const conflictMode = Form.useWatch(['conflictPolicy', 'mode'], { form, preserve: true })
  const retries = Form.useWatch(['errorPolicy', 'maxRetries'], { form, preserve: true })
  const parallelism = Form.useWatch(['writeParams', 'parallelism'], {
    form,
    preserve: true,
  })
  const ddlOperation = Form.useWatch(['operation_filter', 'ddl'], { form, preserve: true })
  const snapshot =
    initMode === undefined ||
    initMode === 'full_only' ||
    initMode === 'full_and_incremental'
  const incremental =
    initMode === undefined || (initMode !== 'full_only' && initMode !== 'schema_only')
  const data = initMode !== 'schema_only'
  const ddl =
    initMode === undefined ||
    initMode === 'schema_only' ||
    (incremental && ddlOperation === true)

  const [replicaSearch, setReplicaSearch] = useState('')
  const endpoints = useEndpointsQuery(
    { page: 1, pageSize: 100, keyword: replicaSearch || undefined },
    open && snapshot && replicaEnabled === true,
  )

  return (
    <Collapse
      defaultActiveKey={['objectFilter', 'operationFilter']}
      items={[
        {
          key: 'mappings',
          label: t('strategies.editor.categories.mappings'),
          children: <MappingRuleFields incremental={incremental} />,
        },
        {
          key: 'transforms',
          label: (
            <span className="section-title">
              {t('strategies.editor.categories.transforms')}
              <HintIcon content={t('strategies.editor.fields.transformHint')} />
            </span>
          ),
          children: <TransformRuleFields />,
        },
        {
          key: 'objectFilter',
          label: (
            <span className="section-title">
              {t('strategies.editor.categories.objectFilter')}
              <HintIcon content={t('strategies.editor.fields.objectFilterHint')} />
            </span>
          ),
          children: (
            <>
              <Form.List name="object_filters">
                {(fields, { add, remove }) => (
                  <>
                    {fields.map((field) => (
                      <Space
                        key={field.key}
                        align="baseline"
                        wrap
                        style={{ display: 'flex', marginBottom: 8 }}
                      >
                        <HintForm name={[field.name, 'action']} noStyle>
                          <Select
                            style={{ width: 90 }}
                            options={[
                              {
                                value: 'FILTER_ACTION_INCLUDE',
                                label: t('strategies.editor.fields.objectFilterActionInclude'),
                              },
                              {
                                value: 'FILTER_ACTION_EXCLUDE',
                                label: t('strategies.editor.fields.objectFilterActionExclude'),
                              },
                            ]}
                          />
                        </HintForm>
                        <HintForm name={[field.name, 'schema_match']} noStyle>
                          <Select
                            style={{ width: 140 }}
                            options={[
                              {
                                value: 'MATCH_TYPE_EXACT',
                                label: t('strategies.editor.fields.objectFilterSchemaExact'),
                              },
                              {
                                value: 'MATCH_TYPE_GLOB',
                                label: t('strategies.editor.fields.objectFilterSchemaGlob'),
                              },
                              {
                                value: 'MATCH_TYPE_REGEX',
                                label: t('strategies.editor.fields.objectFilterSchemaRegex'),
                              },
                            ]}
                          />
                        </HintForm>
                        <HintForm name={[field.name, 'schema_pattern']} noStyle>
                          <Input
                            style={{ width: 160 }}
                            placeholder={t(
                              'strategies.editor.fields.objectFilterSchemaPatternPlaceholder',
                            )}
                          />
                        </HintForm>
                        <HintForm name={[field.name, 'table_match']} noStyle>
                          <Select
                            onChange={(mode) => {
                              if (mode === 'MATCH_TYPE_UNSPECIFIED')
                                form.setFieldValue(
                                  ['object_filters', field.name, 'table_pattern'],
                                  '',
                                )
                            }}
                            style={{ width: 150 }}
                            options={[
                              {
                                value: 'MATCH_TYPE_UNSPECIFIED',
                                label: t('strategies.editor.fields.objectFilterTableUnspecified'),
                              },
                              {
                                value: 'MATCH_TYPE_EXACT',
                                label: t('strategies.editor.fields.objectFilterTableExact'),
                              },
                              {
                                value: 'MATCH_TYPE_GLOB',
                                label: t('strategies.editor.fields.objectFilterTableGlob'),
                              },
                              {
                                value: 'MATCH_TYPE_REGEX',
                                label: t('strategies.editor.fields.objectFilterTableRegex'),
                              },
                            ]}
                          />
                        </HintForm>
                        <HintForm
                          noStyle
                          shouldUpdate={(before, after) =>
                            before.object_filters?.[field.name]?.table_match !==
                            after.object_filters?.[field.name]?.table_match
                          }
                        >
                          {() =>
                            form.getFieldValue(['object_filters', field.name, 'table_match']) !==
                              'MATCH_TYPE_UNSPECIFIED' && (
                              <HintForm name={[field.name, 'table_pattern']} noStyle>
                                <Input
                                  style={{ width: 160 }}
                                  placeholder={t(
                                    'strategies.editor.fields.objectFilterTablePatternPlaceholder',
                                  )}
                                />
                              </HintForm>
                            )
                          }
                        </HintForm>
                        <MinusCircleOutlined
                          title={t('common.delete')}
                          onClick={() => remove(field.name)}
                        />
                      </Space>
                    ))}
                    <Space>
                      <Button
                        type="dashed"
                        icon={<PlusOutlined />}
                        onClick={() => add(createEmptyObjectFilterRule())}
                      >
                        {t('strategies.editor.fields.objectFilterAddRule')}
                      </Button>
                      {fields.length === 0 && (
                        <HintIcon
                          content={<>{t('strategies.editor.fields.objectFilterEmpty')}</>}
                        />
                      )}
                    </Space>
                  </>
                )}
              </Form.List>
            </>
          ),
        },
        {
          key: 'rowFilter',
          label: (
            <span className="section-title">
              {t('strategies.editor.categories.rowFilter')}
              <HintIcon content={t('strategies.editor.fields.rowFilterHint')} />
            </span>
          ),
          children: (
            <>
              <Form.List name="row_filters">
                {(fields, { add, remove }) => (
                  <>
                    {fields.map((field) => (
                      <Space
                        key={field.key}
                        wrap
                        align="baseline"
                        style={{ display: 'flex', marginBottom: 8 }}
                      >
                        <HintForm
                          name={[field.name, 'schema']}
                          rules={[{ required: true }]}
                          noStyle
                        >
                          <Input
                            aria-label={t('strategies.editor.fields.rowFilterSchema')}
                            placeholder={t('strategies.editor.fields.rowFilterSchema')}
                            maxLength={64}
                            style={{ width: 160 }}
                          />
                        </HintForm>
                        <HintForm
                          name={[field.name, 'table']}
                          rules={[{ required: true }]}
                          noStyle
                        >
                          <Input
                            aria-label={t('strategies.editor.fields.rowFilterTable')}
                            placeholder={t('strategies.editor.fields.rowFilterTable')}
                            maxLength={64}
                            style={{ width: 160 }}
                          />
                        </HintForm>
                        <LuaRowFilterFields index={field.name} />
                        <HintForm
                          name={[field.name, 'expression']}
                          rules={[{ required: true }]}
                          noStyle
                        >
                          <Input.TextArea
                            autoSize={{ minRows: 1, maxRows: 6 }}
                            aria-label={t('strategies.editor.fields.rowFilterExpression')}
                            placeholder="age >= 18"
                            maxLength={4096}
                            style={{ width: 260 }}
                          />
                        </HintForm>
                        <Button
                          type="text"
                          aria-label={t('strategies.editor.fields.rowFilterRemove')}
                          icon={<MinusCircleOutlined />}
                          onClick={() => remove(field.name)}
                        />
                      </Space>
                    ))}
                    <Button
                      type="dashed"
                      disabled={fields.length >= 128}
                      icon={<PlusOutlined />}
                      onClick={() => add({ schema: '', table: '', expression: '' })}
                    >
                      {t('strategies.editor.fields.rowFilterAdd')}
                    </Button>
                  </>
                )}
              </Form.List>
            </>
          ),
        },
        {
          key: 'operationFilter',
          label: t('strategies.editor.categories.operationFilter'),
          children: (
            <>
              <HintForm
                name={['operation_filter', 'insert']}
                valuePropName="checked"
                noStyle
              >
                <Checkbox>{t('strategies.editor.fields.operationInsert')}</Checkbox>
              </HintForm>
              <HintForm
                name={['operation_filter', 'update']}
                valuePropName="checked"
                noStyle
              >
                <Checkbox>{t('strategies.editor.fields.operationUpdate')}</Checkbox>
              </HintForm>
              <HintForm
                name={['operation_filter', 'delete']}
                valuePropName="checked"
                noStyle
              >
                <Checkbox>{t('strategies.editor.fields.operationDelete')}</Checkbox>
              </HintForm>
              <HintForm name={['operation_filter', 'ddl']} valuePropName="checked" noStyle>
                <Checkbox>{t('strategies.editor.fields.operationDdl')}</Checkbox>
              </HintForm>
            </>
          ),
        },
        {
          key: 'originPolicy',
          label: t('strategies.editor.categories.originPolicy'),
          children: (
            <>
              <HintForm
                name={['origin_policy', 'mode']}
                label={t('strategies.editor.fields.originMode')}
              >
                <Select
                  onChange={(mode) => {
                    if (
                      mode === 'ORIGIN_POLICY_MODE_LOCAL_ONLY' ||
                      mode === 'ORIGIN_POLICY_MODE_ALL'
                    )
                      form.setFieldValue(['origin_policy', 'centers'], [])
                  }}
                  options={[
                    {
                      value: 'ORIGIN_POLICY_MODE_LOCAL_ONLY',
                      label: t('strategies.editor.fields.originModeLocalOnly'),
                    },
                    {
                      value: 'ORIGIN_POLICY_MODE_INCLUDE_ORIGINS',
                      label: t('strategies.editor.fields.originModeInclude'),
                    },
                    {
                      value: 'ORIGIN_POLICY_MODE_EXCLUDE_ORIGINS',
                      label: t('strategies.editor.fields.originModeExclude'),
                    },
                    {
                      value: 'ORIGIN_POLICY_MODE_ALL',
                      label: t('strategies.editor.fields.originModeAll'),
                    },
                  ]}
                />
              </HintForm>
              {(originMode === 'ORIGIN_POLICY_MODE_INCLUDE_ORIGINS' ||
                originMode === 'ORIGIN_POLICY_MODE_EXCLUDE_ORIGINS') && (
                <HintForm
                  name={['origin_policy', 'centers']}
                  label={t('strategies.editor.fields.originSet')}
                >
                  <Select mode="tags" open={false} />
                </HintForm>
              )}
              <HintForm
                name={['origin_policy', 'max_hops']}
                label={t('strategies.editor.fields.originMaxHops')}
              >
                <InputNumber min={0} style={{ width: '100%' }} />
              </HintForm>
              <HintForm
                name={['origin_policy', 'drop_unknown_origin']}
                valuePropName="checked"
              >
                <Checkbox>{t('strategies.editor.fields.originDropUnknown')}</Checkbox>
              </HintForm>
            </>
          ),
        },
        {
          key: 'ddlPolicy',
          label: t('strategies.editor.categories.ddlPolicy'),
          children: (
            <>
              <HintForm
                name={['ddlPolicy', 'mode']}
                label={t('strategies.editor.fields.ddlMode')}
              >
                <Select
                  options={[
                    { value: 'sync', label: t('strategies.editor.fields.ddlModeSync') },
                    { value: 'ignore', label: t('strategies.editor.fields.ddlModeIgnore') },
                    {
                      value: 'from_origins',
                      label: t('strategies.editor.fields.ddlModeFromOrigins'),
                    },
                  ]}
                />
              </HintForm>
              {ddlMode === 'from_origins' && (
                <HintForm
                  name={['ddlPolicy', 'fromOrigins']}
                  label={t('strategies.editor.fields.ddlFromOrigins')}
                >
                  <Select mode="tags" open={false} />
                </HintForm>
              )}
              {ddlMode !== 'ignore' && (
                <>
                  <HintForm
                    name={['ddlPolicy', 'definerFallback']}
                    label={t('strategies.editor.fields.ddlDefinerFallback')}
                    extra={t('strategies.editor.fields.ddlDefinerHint')}
                  >
                    <Input
                      aria-label={t('strategies.editor.fields.ddlDefinerFallback')}
                      maxLength={512}
                    />
                  </HintForm>
                  <HintForm
                    name={['ddlPolicy', 'blockDangerous']}
                    valuePropName="checked"
                    noStyle
                  >
                    <Checkbox>{t('strategies.editor.fields.ddlBlockDangerous')}</Checkbox>
                  </HintForm>
                </>
              )}
            </>
          ),
        },
        {
          key: 'conflictPolicy',
          label: t('strategies.editor.categories.conflictPolicy'),
          children: (
            <>
              <HintForm
                name={['conflictPolicy', 'mode']}
                label={t('strategies.editor.fields.conflictMode')}
                extra={
                  conflictMode === 'source_wins'
                    ? t('strategies.editor.fields.conflictSourceWinsHint')
                    : conflictMode === 'last_write_wins'
                      ? t('strategies.editor.fields.conflictLwwHint')
                      : undefined
                }
              >
                <Select
                  options={[
                    {
                      value: 'source_wins',
                      label: t('strategies.editor.fields.conflictModeSourceWins'),
                    },
                    {
                      value: 'last_write_wins',
                      label: t('strategies.editor.fields.conflictModeLastWriteWins'),
                    },
                    {
                      value: 'stop',
                      label: t('strategies.editor.fields.conflictModeStop'),
                    },
                  ]}
                />
              </HintForm>
              {conflictMode === 'last_write_wins' && (
                <>
                  <Form.List name={['conflictPolicy', 'lwwTables']}>
                    {(fields, { add, remove }) => (
                      <Space orientation="vertical" style={{ width: '100%' }}>
                        {fields.map((field) => (
                          <Space key={field.key} align="start" wrap>
                            <HintForm
                              name={[field.name, 'schema']}
                              label={t('strategies.editor.fields.conflictLwwSchema')}
                            >
                              <Input />
                            </HintForm>
                            <HintForm
                              name={[field.name, 'table']}
                              label={t('strategies.editor.fields.conflictLwwTable')}
                            >
                              <Input />
                            </HintForm>
                            <HintForm
                              name={[field.name, 'timestampColumn']}
                              label={t('strategies.editor.fields.conflictLwwColumn')}
                            >
                              <Input />
                            </HintForm>
                            <Button
                              aria-label={t('strategies.editor.fields.conflictLwwRemove')}
                              icon={<MinusCircleOutlined />}
                              onClick={() => remove(field.name)}
                            />
                          </Space>
                        ))}
                        <Button
                          icon={<PlusOutlined />}
                          onClick={() =>
                            add({ schema: '', table: '', timestampColumn: '' })
                          }
                        >
                          {t('strategies.editor.fields.conflictLwwAdd')}
                        </Button>
                      </Space>
                    )}
                  </Form.List>
                </>
              )}
              <Form.List name={['conflictPolicy', 'maskRules']}>
                {(fields, { add, remove }) => (
                  <Space orientation="vertical" style={{ width: '100%' }}>
                    {fields.map((field) => (
                      <Space key={field.key} align="start" wrap>
                        <HintForm
                          name={[field.name, 'schema']}
                          label={t('strategies.editor.fields.conflictMaskSchema')}
                        >
                          <Input />
                        </HintForm>
                        <HintForm
                          name={[field.name, 'table']}
                          label={t('strategies.editor.fields.conflictMaskTable')}
                        >
                          <Input />
                        </HintForm>
                        <HintForm
                          name={[field.name, 'columns']}
                          label={t('strategies.editor.fields.conflictMaskColumns')}
                        >
                          <Select
                            mode="tags"
                            tokenSeparators={[',']}
                            open={false}
                            style={{ minWidth: 220 }}
                          />
                        </HintForm>
                        <Button
                          aria-label={t('strategies.editor.fields.conflictMaskRemove')}
                          icon={<MinusCircleOutlined />}
                          onClick={() => remove(field.name)}
                        />
                      </Space>
                    ))}
                    <span className="section-title">
                      <Button
                        icon={<PlusOutlined />}
                        onClick={() => add({ schema: '', table: '', columns: [] })}
                      >
                        {t('strategies.editor.fields.conflictMaskAdd')}
                      </Button>
                      <HintIcon content={t('strategies.editor.fields.conflictMaskHint')} />
                    </span>
                  </Space>
                )}
              </Form.List>
            </>
          ),
        },
        {
          key: 'errorPolicy',
          label: t('strategies.editor.categories.errorPolicy'),
          children: (
            <>
              <HintForm
                name={['errorPolicy', 'maxRetries']}
                label={t('strategies.editor.fields.errorMaxRetries')}
              >
                <InputNumber min={0} style={{ width: '100%' }} />
              </HintForm>
              {retries > 0 && (
                <HintForm
                  name={['errorPolicy', 'backoffMs']}
                  label={t('strategies.editor.fields.errorBackoffMs')}
                >
                  <InputNumber min={0} style={{ width: '100%' }} />
                </HintForm>
              )}
              <HintForm
                name={['errorPolicy', 'skipAndLogOnNonRetryable']}
                valuePropName="checked"
                noStyle
              >
                <Checkbox>{t('strategies.editor.fields.errorSkipOnNonRetryable')}</Checkbox>
              </HintForm>
            </>
          ),
        },
        {
          key: 'snapshotParams',
          label: t('strategies.editor.categories.snapshotParams'),
          children: (
            <>
              <HintForm
                name={['snapshotParams', 'readConcurrency']}
                label={t('strategies.editor.fields.snapshotReadConcurrency')}
              >
                <InputNumber min={1} style={{ width: '100%' }} />
              </HintForm>
              <HintForm
                name={['snapshotParams', 'chunkRows']}
                label={t('strategies.editor.fields.snapshotChunkRows')}
              >
                <InputNumber min={1} style={{ width: '100%' }} />
              </HintForm>
              <HintForm
                name={['snapshotParams', 'maxRowsPerTxn']}
                label={t('strategies.editor.fields.snapshotMaxRowsPerTxn')}
              >
                <InputNumber min={1} style={{ width: '100%' }} />
              </HintForm>
              <HintForm
                name={['snapshotParams', 'maxBytesPerTxn']}
                label={t('strategies.editor.fields.snapshotMaxBytesPerTxn')}
              >
                <InputNumber min={1} style={{ width: '100%' }} />
              </HintForm>
              <HintForm
                name={['snapshotParams', 'writeConcurrency']}
                label={t('strategies.editor.fields.snapshotWriteConcurrency')}
                extra={t('strategies.editor.fields.snapshotWriteConcurrencyHint')}
              >
                <InputNumber min={1} style={{ width: '100%' }} />
              </HintForm>
              <HintForm
                name={['snapshotParams', 'rateLimit']}
                label={t('strategies.editor.fields.snapshotRateLimit')}
              >
                <InputNumber min={1} style={{ width: '100%' }} />
              </HintForm>
              <HintForm
                name={['snapshotParams', 'readFromReplica']}
                valuePropName="checked"
                noStyle
              >
                <Checkbox
                  onChange={(event) => {
                    if (!event.target.checked)
                      form.setFieldValue(['snapshotParams', 'replicaEndpointId'], undefined)
                  }}
                >
                  {t('strategies.editor.fields.snapshotReadFromReplica')}
                </Checkbox>
              </HintForm>
              {replicaEnabled && (
                <HintForm
                  name={['snapshotParams', 'replicaEndpointId']}
                  label={t('strategies.editor.fields.snapshotReplicaEndpoint')}
                  extra={t('strategies.editor.fields.snapshotReplicaEndpointHint')}
                >
                  <Select
                    showSearch={{ filterOption: false, onSearch: setReplicaSearch }}
                    loading={endpoints.isLoading}
                    allowClear
                    options={(endpoints.data?.items ?? [])
                      .filter((endpoint) => endpoint.serverUuid && endpoint.lowerCaseTableNames)
                      .map((endpoint) => ({
                        value: endpoint.id,
                        label: `${endpoint.name} (${endpoint.host}:${endpoint.port})`,
                      }))}
                  />
                </HintForm>
              )}
            </>
          ),
        },
        {
          key: 'writeParams',
          label: t('strategies.editor.categories.writeParams'),
          children: (
            <>
              {incremental && (
                <>
                  <HintForm
                    name={['incremental_window', 'max_batches']}
                    label={t('strategies.editor.fields.publicationWindowBatches')}
                    extra={t('strategies.editor.fields.publicationWindowHint')}
                  >
                    <InputNumber
                      min={1}
                      max={128}
                      placeholder="1"
                      style={{ width: '100%' }}
                    />
                  </HintForm>
                  <HintForm
                    name={['incremental_window', 'max_bytes']}
                    label={t('strategies.editor.fields.publicationWindowBytes')}
                  >
                    <InputNumber
                      min={1}
                      max={1073741824}
                      placeholder="67108864"
                      style={{ width: '100%' }}
                    />
                  </HintForm>
                  <HintForm
                    name={['incremental_window', 'reorder_timeout_ms']}
                    label={t('strategies.editor.fields.publicationWindowTimeout')}
                  >
                    <InputNumber
                      min={100}
                      max={600000}
                      placeholder="60000"
                      style={{ width: '100%' }}
                    />
                  </HintForm>
                </>
              )}
              <HintForm
                name={['writeParams', 'batchSize']}
                label={t('strategies.editor.fields.writeBatchSize')}
              >
                <InputNumber min={0} max={10000} style={{ width: '100%' }} />
              </HintForm>
              <HintForm
                name={['writeParams', 'parallelism']}
                label={t('strategies.editor.fields.writeParallelism')}
              >
                <InputNumber min={0} max={128} style={{ width: '100%' }} />
              </HintForm>
              {parallelism > 1 && (
                <HintForm name={['writeParams', 'orderedCommit']} valuePropName="checked">
                  <Checkbox>{t('strategies.editor.fields.writeOrderedCommit')}</Checkbox>
                </HintForm>
              )}
              <HintForm
                name={['writeParams', 'mergeRowsInSameTable']}
                valuePropName="checked"
                noStyle
              >
                <Checkbox>{t('strategies.editor.fields.writeMergeRows')}</Checkbox>
              </HintForm>
            </>
          ),
        },
      ].filter((item) => {
        if (['operationFilter', 'originPolicy'].includes(item.key)) return incremental
        if (item.key === 'snapshotParams') return snapshot
        if (item.key === 'ddlPolicy') return ddl
        if (item.key === 'writeParams') return incremental
        if (['rowFilter', 'transforms', 'conflictPolicy'].includes(item.key)) return data
        return true
      })}
    />
  )
}
