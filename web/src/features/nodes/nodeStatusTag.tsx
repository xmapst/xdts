import { Tag } from 'antd'
import { useTranslation } from 'react-i18next'
import type { NodeStatus } from '@/api/nodes'
import { enumLabel } from '@/utils/i18nEnum'

const STATUS_COLOR: Record<NodeStatus, string> = {
  registering: 'processing',
  online: 'success',
  offline: 'default',
  disabled: 'warning',

  revoked: 'error',
}

/** 节点状态标签：在列表与详情页共用同一份颜色 / 文案映射。 */
export function NodeStatusTag({ status }: { status: NodeStatus }) {
  const { t } = useTranslation()
  // status 来自后端 model.Node.status。
  return <Tag color={STATUS_COLOR[status]}>{enumLabel(t, 'nodes.status', status)}</Tag>
}
