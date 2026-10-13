import { ScrollTable } from '@/components/ScrollTable'
import { useHintFeedback } from '@/components/hintFeedback'
import { HintIcon } from '@/components/HintIcon'
import { useState } from 'react'
import { Button, Input, Select, Space, Tag } from 'antd'
import { PlusOutlined } from '@ant-design/icons'
import { useTranslation } from 'react-i18next'
import type { AuthSource, UserRole } from '@/api/auth'
import {
  useUsersQuery,
  useSetUserStatusMutation,
  useUnlockUserMutation,
  useRevokeUserSessionsMutation,
  useDeleteUserMutation,
  type UserListQuery,
  type UserListStatus,
  type UserSummary,
} from '@/api/users'
import { RequireRole } from '@/components/RequireRole'
import { ColumnSettingsButton } from '@/components/ColumnSettingsButton'
import { ExportButton } from '@/components/ExportButton'
import { formatDateTime } from '@/utils/format'
import type { ManagedColumn } from '@/utils/tableColumns'
import { useColumnVisibility } from '@/utils/useColumnVisibility'
import { UserFormDrawer } from './UserFormDrawer'
import { ResetPasswordDrawer } from './ResetPasswordDrawer'
import { UserRowActions } from './UserRowActions'
import { UserScopesDrawer } from './UserScopesDrawer'
import { enumLabel } from '@/utils/i18nEnum'

const DEFAULT_PAGE_SIZE = 20
const PAGE_SIZE_OPTIONS = ['10', '20', '50', '100']
const ROLES: UserRole[] = ['admin', 'readonly']
const AUTH_SOURCES: AuthSource[] = ['local', 'ldap', 'oidc']
const STATUSES: UserListStatus[] = ['active', 'disabled']

const STATUS_COLOR: Record<UserListStatus, string> = {
  active: 'success',
  disabled: 'warning',
}
const AUTH_SOURCE_COLOR: Record<AuthSource, string> = {
  local: 'default',
  ldap: 'processing',
  oidc: 'purple',
}

export default function UsersPage() {
  const { t } = useTranslation()
  const { message } = useHintFeedback()
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(DEFAULT_PAGE_SIZE)
  const [keyword, setKeyword] = useState('')
  const [role, setRole] = useState<UserRole | undefined>()
  const [authSource, setAuthSource] = useState<AuthSource | undefined>()
  const [status, setStatus] = useState<UserListStatus | undefined>()

  const [formTarget, setFormTarget] = useState<{ open: boolean; user?: UserSummary }>({
    open: false,
  })
  const [passwordTarget, setPasswordTarget] = useState<{
    open: boolean
    user?: UserSummary
  }>({
    open: false,
  })
  const [scopeTarget, setScopeTarget] = useState<UserSummary>()

  const query: UserListQuery = {
    page,
    pageSize,
    keyword: keyword || undefined,
    role,
    authSource,
    status,
  }
  const usersQuery = useUsersQuery(query)
  const statusMutation = useSetUserStatusMutation()
  const unlockMutation = useUnlockUserMutation()
  const revokeSessionsMutation = useRevokeUserSessionsMutation()
  const deleteMutation = useDeleteUserMutation()

  const handleToggleStatus = (user: UserSummary) => {
    statusMutation.mutate(
      {
        id: user.id,
        input: { status: user.status === 'disabled' ? 'active' : 'disabled' },
      },
      {
        onSuccess: () => void message.success(t('common.operationSucceeded')),
        onError: (error) => void message.error(error.message),
      },
    )
  }

  const handleUnlock = (user: UserSummary) => {
    unlockMutation.mutate(user.id, {
      onSuccess: () => void message.success(t('common.operationSucceeded')),
      onError: (error) => void message.error(error.message),
    })
  }

  const handleRevokeSessions = (user: UserSummary) => {
    revokeSessionsMutation.mutate(user.id, {
      onSuccess: (result) =>
        void message.success(
          t('users.actions.revokeSessionsSucceeded', { count: result.revokedCount }),
        ),
      onError: (error) => void message.error(error.message),
    })
  }

  const handleDelete = (user: UserSummary) => {
    deleteMutation.mutate(user.id, {
      onSuccess: () => void message.success(t('common.operationSucceeded')),
      onError: (error) => void message.error(error.message),
    })
  }

  const columns: ManagedColumn<UserSummary>[] = [
    {
      key: 'username',
      title: t('users.columns.username'),
      dataIndex: 'username',
      hideable: false,
    },
    {
      key: 'role',
      title: t('users.columns.role'),
      dataIndex: 'role',
      // role / status / authSource 三个都来自后端 users 表，两路都兜。
      render: (value: UserRole) => <Tag>{enumLabel(t, 'common.role', value)}</Tag>,
      exportValue: (record) => enumLabel(t, 'common.role', record.role),
    },
    {
      key: 'status',
      title: t('users.columns.status'),
      dataIndex: 'status',
      render: (value: UserListStatus) => (
        <Tag color={STATUS_COLOR[value]}>{enumLabel(t, 'users.status', value)}</Tag>
      ),
      exportValue: (record) => enumLabel(t, 'users.status', record.status),
    },
    {
      key: 'authSource',
      title: t('users.columns.authSource'),
      dataIndex: 'authSource',
      render: (value: AuthSource) => (
        <Tag color={AUTH_SOURCE_COLOR[value]}>
          {enumLabel(t, 'users.authSource', value)}
        </Tag>
      ),
      exportValue: (record) => enumLabel(t, 'users.authSource', record.authSource),
    },
    {
      key: 'lastLoginAt',
      title: t('users.columns.lastLoginAt'),
      dataIndex: 'lastLoginAt',
      render: (value: string | null) => formatDateTime(value),
      exportValue: (record) => formatDateTime(record.lastLoginAt),
    },
    {
      key: 'createdAt',
      title: t('users.columns.createdAt'),
      dataIndex: 'createdAt',
      render: (value: string) => formatDateTime(value),
      exportValue: (record) => formatDateTime(record.createdAt),
    },
    {
      key: 'actions',
      title: t('common.actions'),
      hideable: false,
      render: (_value, record) => (
        <UserRowActions
          onScopes={() => setScopeTarget(record)}
          user={record}
          onEdit={() => setFormTarget({ open: true, user: record })}
          onResetPassword={() => setPasswordTarget({ open: true, user: record })}
          onToggleStatus={() => handleToggleStatus(record)}
          onUnlock={() => handleUnlock(record)}
          onRevokeSessions={() => handleRevokeSessions(record)}
          onDelete={() => handleDelete(record)}
        />
      ),
    },
  ]
  const { visibleColumns, pickerProps } = useColumnVisibility('users', columns)

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%', minHeight: 0 }}>
      <Space
        style={{
          marginBottom: 16,
          width: '100%',
          justifyContent: 'space-between',
          flexShrink: 0,
        }}
      >
        <Space wrap>
          <Select
            allowClear
            placeholder={t('users.filterRole')}
            style={{ width: 140 }}
            value={role}
            onChange={(value) => {
              setRole(value)
              setPage(1)
            }}
            options={ROLES.map((item) => ({
              value: item,
              label: t(`common.role.${item}`),
            }))}
          />
          <Select
            allowClear
            placeholder={t('users.filterAuthSource')}
            style={{ width: 160 }}
            value={authSource}
            onChange={(value) => {
              setAuthSource(value)
              setPage(1)
            }}
            options={AUTH_SOURCES.map((item) => ({
              value: item,
              label: t(`users.authSource.${item}`),
            }))}
          />
          <Select
            allowClear
            placeholder={t('users.filterStatus')}
            style={{ width: 140 }}
            value={status}
            onChange={(value) => {
              setStatus(value)
              setPage(1)
            }}
            options={STATUSES.map((item) => ({
              value: item,
              label: t(`users.status.${item}`),
            }))}
          />
          <Input.Search
            allowClear
            placeholder={t('users.searchPlaceholder')}
            style={{ width: 220 }}
            onSearch={(value) => {
              setKeyword(value)
              setPage(1)
            }}
          />
        </Space>
        <Space wrap>
          {usersQuery.isError && (
            <HintIcon type="error" content={<> {usersQuery.error.message} </>} />
          )}
          <ExportButton
            filename="users"
            columns={visibleColumns}
            rows={usersQuery.data?.items ?? []}
            scope="page"
          />
          <ColumnSettingsButton {...pickerProps} />
          <RequireRole role="admin">
            <Button
              type="primary"
              icon={<PlusOutlined />}
              onClick={() => setFormTarget({ open: true })}
            >
              {t('users.create')}
            </Button>
          </RequireRole>
        </Space>
      </Space>

      <div
        className="table-fill-container"
        style={{ flex: '1 1 auto', minHeight: 0, overflow: 'hidden' }}
      >
        <ScrollTable<UserSummary>
          rowKey="id"
          loading={usersQuery.isLoading}
          dataSource={usersQuery.data?.items ?? []}
          pagination={{
            current: page,
            pageSize,
            total: usersQuery.data?.total ?? 0,
            showSizeChanger: true,
            showQuickJumper: true,
            pageSizeOptions: PAGE_SIZE_OPTIONS,
            onChange: (nextPage, nextPageSize) => {
              setPage(nextPage)
              setPageSize(nextPageSize)
            },
          }}
          columns={visibleColumns}
        />
      </div>

      <UserFormDrawer
        open={formTarget.open}
        user={formTarget.user}
        onClose={() => setFormTarget({ open: false })}
      />
      <ResetPasswordDrawer
        open={passwordTarget.open}
        user={passwordTarget.user}
        onClose={() => setPasswordTarget({ open: false })}
      />
      {scopeTarget && (
        <UserScopesDrawer
          key={scopeTarget.id}
          user={scopeTarget}
          onClose={() => setScopeTarget(undefined)}
        />
      )}
    </div>
  )
}
