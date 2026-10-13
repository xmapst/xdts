import { HintIcon } from '@/components/HintIcon'
import { useTranslation } from 'react-i18next'
import type { UserSummary } from '@/api/users'
import { useCurrentUser } from '@/api/auth'
import { RequireRole } from '@/components/RequireRole'
import { RowActions } from '@/components/RowActions'

interface UserRowActionsProps {
  user: UserSummary
  onEdit: () => void
  onResetPassword: () => void
  onToggleStatus: () => void
  onUnlock: () => void
  onRevokeSessions: () => void
  onDelete: () => void
  onScopes?: () => void
}

export function UserRowActions({
  user,
  onEdit,
  onResetPassword,
  onToggleStatus,
  onUnlock,
  onRevokeSessions,
  onDelete,
  onScopes,
}: UserRowActionsProps) {
  const { t } = useTranslation()
  const { user: currentUser } = useCurrentUser()
  const isSelf = Boolean(currentUser) && user.id === currentUser?.id
  const isLocal = user.authSource === 'local'
  const isDisabled = user.status === 'disabled'

  const selfHint = (label: string) => (
    <span>
      {label} <HintIcon type="warning" content={t('users.actions.cannotModifySelf')} />
    </span>
  )

  return (
    <RequireRole role="admin">
      <RowActions
        // Keep all ordinary actions inline so self-protection does not move disabled actions into the overflow menu.
        maxInline={4}
        actions={[
          { key: 'edit', label: t('common.edit'), onClick: onEdit },
          {
            key: 'resetPassword',
            label: t('users.actions.resetPassword'),
            onClick: onResetPassword,
            // 外部账号密码不归本系统管，不展示这个入口（）。
            visible: isLocal,
          },
          {
            key: 'toggleStatus',
            label: isSelf
              ? selfHint(t(isDisabled ? 'users.actions.enable' : 'users.actions.disable'))
              : t(isDisabled ? 'users.actions.enable' : 'users.actions.disable'),
            onClick: onToggleStatus,
            disabled: isSelf,
            confirm: isSelf
              ? undefined
              : {
                  title: t(
                    isDisabled ? 'users.actions.enableConfirm' : 'users.actions.disableConfirm',
                  ),
                },
          },
          {
            key: 'unlock',
            label: t('users.actions.unlock'),
            onClick: onUnlock,
            // 幂等操作（）：列表页看不到是否锁定（不做批量锁定
            // 查询，UserSummary.status 只有 active/disabled 两态），这个入口
            // 因此对所有行常驻展示，未锁定时点击也不报错，不需要按状态隐藏。
          },
          {
            key: 'scopes',
            label: t('users.scopes.action'),
            onClick: onScopes,
            visible: user.role === 'readonly' && Boolean(onScopes),
          },
          {
            key: 'revokeSessions',
            label: t('users.actions.revokeSessions'),
            danger: true,
            onClick: onRevokeSessions,
            confirm: { title: t('users.actions.revokeSessionsConfirm') },
          },
          {
            key: 'delete',
            label: isSelf ? selfHint(t('common.delete')) : t('common.delete'),
            danger: true,
            disabled: isSelf,
            // 不依赖 RowActions 内部对 danger 分支的 disabled 处理——它的
            // onClick 只看 confirm 是否存在，不看 disabled（web/src/components/RowActions.tsx
            // renderActionNode 的 danger 分支：`onClick={action.confirm ?
            // undefined : action.onClick}`，没有把 disabled 也并进这个判断）。
            // 这里自己在回调层面再挡一次，不完全依赖 antd Typography.Link
            // 的 disabled 视觉态恰好也拦住点击——双重保险，且不改动共享组件
            onClick: isSelf ? undefined : onDelete,
            confirm: isSelf ? undefined : { title: t('users.deleteConfirm') },
          },
        ]}
      />
    </RequireRole>
  )
}
