import { ActionDrawer } from '@/components/ActionDrawer'
import { useHintFeedback } from '@/components/hintFeedback'
import { HintAlert as Alert } from '@/components/HintAlert'
import { HintForm } from '@/components/HintForm'
import { useEffect, useState } from 'react'
import { Button, Form, Select, Space } from 'antd'
import { useTranslation } from 'react-i18next'
import {
  useEndpointQuery,
  useProbeEndpointMutation,
  type EndpointSummary,
} from '@/api/endpoints'
import { useNodesQuery } from '@/api/nodes'
import { ProbeResultPanel } from './ProbeResultPanel'

interface ProbeConnectionDrawerProps {
  open: boolean
  onClose: () => void
  endpoint: EndpointSummary | undefined
}

/**
 * 「测试连接」抽屉（MGR-DS-02）：抽屉只负责开关，交互状态全部收在
 * ProbeConnectionDrawerBody 里，并用 `key={endpoint.id}` 让每次针对不同数据
 * 端点重新打开时天然重置状态（React 官方推荐的「用 key 重置状态」模式），不需要
 * 额外的「监听 open 变化后手动清空状态」的 effect。
 */
export function ProbeConnectionDrawer({
  open,
  onClose,
  endpoint,
}: ProbeConnectionDrawerProps) {
  const { t } = useTranslation()
  return (
    <ActionDrawer
      open={open}
      title={t('dataEndpoints.probe.title')}
      onClose={onClose}
      footer={null}
      destroyOnHidden
    >
      {endpoint && (
        <ProbeConnectionDrawerBody key={endpoint.id} endpoint={endpoint} open={open} />
      )}
    </ActionDrawer>
  )
}

/** 探测轮询的间隔与超时：节点执行 + 回执有网络与任务队列耗时，不是即时的，
 * 界面必须能表达「派发中」这个中间状态（团队要求：派发 → 轮询 / 等待结果 →
 * 展示，不能假装同步完成）。数值是前端体验层面的选择，不是后端契约。 */
const POLL_INTERVAL_MS = 2000
const POLL_TIMEOUT_MS = 30_000

interface ProbeConnectionDrawerBodyProps {
  endpoint: EndpointSummary
  open: boolean
}

function ProbeConnectionDrawerBody({ endpoint, open }: ProbeConnectionDrawerBodyProps) {
  const { t } = useTranslation()
  const { message } = useHintFeedback()
  const [form] = Form.useForm<{ nodeId: string }>()
  const probeMutation = useProbeEndpointMutation()

  const [dispatchedTaskId, setDispatchedTaskId] = useState<string | null>(null)
  const [timedOut, setTimedOut] = useState(false)

  // 只接受与本次派发 taskId 相同的回执，避免时钟偏差及后台自动探测误判。
  const endpointQueryPreArrival = useEndpointQuery(open ? endpoint.id : undefined, false)
  const arrived = Boolean(
    dispatchedTaskId &&
      endpointQueryPreArrival.data?.lastProbeResult?.taskId === dispatchedTaskId,
  )
  const polling = dispatchedTaskId !== null && !arrived && !timedOut
  const endpointQuery = useEndpointQuery(
    open ? endpoint.id : undefined,
    polling ? POLL_INTERVAL_MS : false,
  )
  const nodesQuery = useNodesQuery(
    { page: 1, pageSize: 100, centerId: endpoint.centerId, status: 'online' },
    open,
  )

  // 超时判定用独立的墙钟定时器：从派发时刻起计时，与 refetch 节奏无关——即便
  // 某次轮询请求本身卡住没有回来，超时也应该按时触发，给出「重试」而不是无限
  // 转圈（任务带 task_id 与超时）。到点时若已经拿到结果（arrived），
  // 渲染时会优先展示结果，不受这个 timer 的影响。
  useEffect(() => {
    if (!dispatchedTaskId) {
      return
    }
    const timer = window.setTimeout(() => setTimedOut(true), POLL_TIMEOUT_MS)
    return () => window.clearTimeout(timer)
  }, [dispatchedTaskId])

  const nodeOptions = (nodesQuery.data?.items ?? []).map((node) => ({
    value: node.id,
    // hostname 可能为空串（未取得主机信息时），原写法渲染出的是
    // 「名称（）」——一个空括号，看起来像数据没加载完。空时只显示名称。
    label: node.hostname ? `${node.name}（${node.hostname}）` : node.name,
  }))
  const noOnlineNode = !nodesQuery.isLoading && nodeOptions.length === 0

  const handleDispatch = (values: { nodeId: string }) => {
    setTimedOut(false)
    probeMutation.mutate(
      { id: endpoint.id, nodeId: values.nodeId },
      {
        onSuccess: ({ taskId }) => setDispatchedTaskId(taskId),
        onError: (error) => void message.error(error.message),
      },
    )
  }

  const showTimeoutAlert = timedOut && !arrived

  const display = endpointQuery.data ?? endpoint

  const showRetry = dispatchedTaskId !== null || Boolean(display.lastProbeAt)

  return (
    <Space orientation="vertical" style={{ width: '100%' }} size="middle">
      <Form<{ nodeId: string }> form={form} layout="inline" onFinish={handleDispatch}>
        <HintForm
          name="nodeId"
          label={t('dataEndpoints.probe.node')}
          style={{ flex: 1 }}
          rules={[{ required: true, message: t('dataEndpoints.probe.nodeRequired') }]}
        >
          <Select
            style={{ minWidth: 240 }}
            loading={nodesQuery.isLoading}
            placeholder={t('dataEndpoints.probe.nodePlaceholder')}
            disabled={noOnlineNode || polling}
            options={nodeOptions}
          />
        </HintForm>
        <HintForm>
          <Space wrap>
            <Button
              type="primary"
              htmlType="submit"
              loading={probeMutation.isPending || polling}
              disabled={noOnlineNode}
            >
              {showRetry
                ? t('dataEndpoints.probe.retry')
                : t('dataEndpoints.probe.dispatch')}
            </Button>
            {noOnlineNode && (
              <Alert type="warning" showIcon title={t('dataEndpoints.probe.nodeEmpty')} />
            )}

            {polling && (
              <Alert type="info" showIcon title={t('dataEndpoints.probe.dispatching')} />
            )}
            {showTimeoutAlert && (
              <Alert type="error" title={t('dataEndpoints.probe.timeout')} />
            )}
          </Space>
        </HintForm>
      </Form>

      <ProbeResultPanel
        lastProbeAt={display.lastProbeAt}
        result={display.lastProbeResult}
      />
    </Space>
  )
}
