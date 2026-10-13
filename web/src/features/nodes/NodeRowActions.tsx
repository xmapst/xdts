import { HintIcon } from '@/components/HintIcon'
import { Divider, Popconfirm, Space, Typography } from 'antd'
import { useTranslation } from 'react-i18next'
import type { NodeSummary } from '@/api/nodes'
import { RequireRole } from '@/components/RequireRole'

interface NodeRowActionsProps {
  node: Pick<NodeSummary, 'status' | 'hasPipelines'>
  onToggleDisabled: () => void
  onRevokeToken: () => void
  onDeregister: () => void
  onEdit?: () => void
}

/* 节点操作按钮组：禁用 / 启用（MGR-NODE-06）、吊销 token、注销（MGR-NODE-06a）。
共用于列表页与详情页，避免两处各写一份 Popconfirm 文案与角色判断。 */
export function NodeRowActions({
  node,
  onToggleDisabled,
  onRevokeToken,
  onDeregister,
  onEdit,
}: NodeRowActionsProps) {
  const { t } = useTranslation()

  const revoked = node.status === 'revoked'
  const disabled = node.status === 'disabled'
  const inUse = node.hasPipelines !== false

  return (
    <RequireRole role="admin">
      <Space size="small">
        {inUse && <HintIcon type="warning" content={t('nodes.actions.inUseHint')} />}
        {onEdit && <a onClick={onEdit}>{t('nodes.edit.action')}</a>}
        {revoked ? (
          <HintIcon content={<> {t('nodes.actions.revokedHint')} </>} />
        ) : (
          <>
            <Popconfirm
              title={t(
                disabled ? 'nodes.actions.enableConfirm' : 'nodes.actions.disableConfirm',
              )}
              disabled={inUse && !disabled}
              onConfirm={onToggleDisabled}
            >
              <Typography.Link disabled={inUse && !disabled}>
                {t(disabled ? 'nodes.actions.enable' : 'nodes.actions.disable')}
              </Typography.Link>
            </Popconfirm>
            <Popconfirm
              disabled={inUse}
              title={t('nodes.actions.revokeTokenConfirm')}
              onConfirm={onRevokeToken}
            >
              {}
              <Typography.Link disabled={inUse} type="danger">
                {t('nodes.actions.revokeToken')}
              </Typography.Link>
            </Popconfirm>
          </>
        )}
        {/* 注销与前面几项之间加分隔线：它是这一组里唯一会让记录从列表消失的
            操作，和「禁用」「吊销」不是一个量级，紧贴着排会提高误触代价
            （同 RowActions 对 danger 动作的处理）。 */}
        <Divider orientation="vertical" style={{ margin: 0 }} />
        <Popconfirm
          title={t('nodes.actions.deregisterConfirm')}
          okText={t('common.confirm')}
          cancelText={t('common.cancel')}
          disabled={inUse}
          onConfirm={onDeregister}
        >
          <Typography.Link disabled={inUse} type="danger">
            {t('nodes.actions.deregister')}
          </Typography.Link>
        </Popconfirm>
      </Space>
    </RequireRole>
  )
}
