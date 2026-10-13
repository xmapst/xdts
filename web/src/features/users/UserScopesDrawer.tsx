import { ActionDrawer } from '@/components/ActionDrawer'
import { useHintFeedback } from '@/components/hintFeedback'
import { HintAlert as Alert } from '@/components/HintAlert'
import { HintForm } from '@/components/HintForm'
import { Button, Form, Pagination, Select, Switch } from 'antd'
import { useState } from 'react'
import { useTranslation } from 'react-i18next'
import { useCentersQuery } from '@/api/centers'
import { usePipelinesQuery } from '@/api/pipelines'
import {
  useReplaceUserScopesMutation,
  useUserScopesQuery,
  type UserSummary,
} from '@/api/users'

interface ScopeValues {
  restricted: boolean
  centers: string[]
  pipelines: string[]
}

interface UserScopesDrawerProps {
  user: UserSummary
  onClose: () => void
}

// UsersPage mounts one instance per user. Initial values are applied only after
// loading succeeds; background query refreshes cannot overwrite an edit.
export function UserScopesDrawer({ user, onClose }: UserScopesDrawerProps) {
  const { t } = useTranslation()
  const { message } = useHintFeedback()
  const [form] = Form.useForm<ScopeValues>()
  const scopes = useUserScopesQuery(user.id)
  const mutation = useReplaceUserScopesMutation(user.id)
  const [keyword, setKeyword] = useState('')
  const [page, setPage] = useState(1)
  const restricted = Form.useWatch('restricted', form) ?? scopes.data?.restricted ?? false
  const selectedCenters: string[] =
    Form.useWatch('centers', form) ??
    scopes.data?.items
      .filter((item) => item.scope === 'center')
      .map((item) => item.resourceId) ??
    []
  const selectedPipelines: string[] =
    Form.useWatch('pipelines', form) ??
    scopes.data?.items
      .filter((item) => item.scope === 'pipeline')
      .map((item) => item.resourceId) ??
    []
  const centers = useCentersQuery(restricted)
  const pipelines = usePipelinesQuery(
    { page, pageSize: 50, keyword: keyword || undefined },
    restricted,
  )

  const withSelected = (items: { id: string; name: string }[], selected: string[]) => {
    const options = items.map((item) => ({ value: item.id, label: item.name }))
    const present = new Set(items.map((item) => item.id))
    return [
      ...options,
      ...selected.filter((id) => !present.has(id)).map((id) => ({ value: id, label: id })),
    ]
  }

  const submit = (values: ScopeValues) => {
    if (mutation.isPending || !scopes.data || scopes.isError) return
    const grants = values.restricted
      ? [
          ...values.centers.map((resourceId) => ({ scope: 'center' as const, resourceId })),
          ...values.pipelines.map((resourceId) => ({
            scope: 'pipeline' as const,
            resourceId,
          })),
        ]
      : []
    if (grants.length > 1000) {
      void message.error(t('users.scopes.limit'))
      return
    }
    mutation.mutate(
      { restricted: values.restricted, scopes: grants },
      {
        onSuccess: () => {
          void message.success(t('common.operationSucceeded'))
          onClose()
        },
        onError: (error) => void message.error(error.message),
      },
    )
  }

  return (
    <ActionDrawer
      open
      title={t('users.scopes.title', { username: user.username })}
      onClose={() => {
        if (!mutation.isPending) onClose()
      }}
      onConfirm={() => form.submit()}
      confirmText={t('common.submit')}
      cancelText={t('common.cancel')}
      confirmButtonProps={{ disabled: !scopes.data || scopes.isError }}
      cancelButtonProps={{ disabled: mutation.isPending }}
      confirmLoading={mutation.isPending}
      loading={scopes.isPending}
      destroyOnHidden
    >
      {scopes.isError && (
        <Alert
          type="error"
          showIcon
          title={scopes.error.message}
          action={
            <Button onClick={() => void scopes.refetch()} loading={scopes.isFetching}>
              {t('common.retry')}
            </Button>
          }
        />
      )}
      {scopes.data && !scopes.isError && (
        <Form<ScopeValues>
          form={form}
          layout="vertical"
          disabled={mutation.isPending}
          initialValues={{
            restricted: scopes.data.restricted,
            centers: scopes.data.items
              .filter((item) => item.scope === 'center')
              .map((item) => item.resourceId),
            pipelines: scopes.data.items
              .filter((item) => item.scope === 'pipeline')
              .map((item) => item.resourceId),
          }}
          onFinish={submit}
        >
          <HintForm
            name="restricted"
            label={
              <span className="field-label-hint">
                {t('users.scopes.restrict')}
                <Alert type="info" title={t('users.scopes.readonlyHint')} />
                {restricted && selectedCenters.length + selectedPipelines.length === 0 && (
                  <Alert type="warning" title={t('users.scopes.emptyHint')} />
                )}
              </span>
            }
            valuePropName="checked"
          >
            <Switch aria-label={t('users.scopes.restrict')} />
          </HintForm>
          <HintForm
            name="centers"
            label={t('users.scopes.centers')}
            extra={t('users.scopes.centerHint')}
          >
            <Select
              mode="multiple"
              disabled={!restricted || mutation.isPending}
              loading={centers.isFetching}
              options={withSelected(centers.data ?? [], selectedCenters)}
              optionFilterProp="label"
            />
          </HintForm>
          <HintForm name="pipelines" label={t('users.scopes.pipelines')}>
            <Select
              mode="multiple"
              disabled={!restricted || mutation.isPending}
              loading={pipelines.isFetching}
              options={withSelected(pipelines.data?.items ?? [], selectedPipelines)}
              filterOption={false}
              onSearch={(value) => {
                setKeyword(value)
                setPage(1)
              }}
              popupRender={(menu) => (
                <>
                  {menu}
                  <Pagination
                    size="small"
                    current={page}
                    pageSize={50}
                    total={pipelines.data?.total ?? 0}
                    showSizeChanger={false}
                    onChange={setPage}
                    style={{ padding: 8 }}
                  />
                </>
              )}
            />
          </HintForm>
          {(centers.isError || pipelines.isError) && (
            <Alert
              type="error"
              showIcon
              title={t('users.scopes.resourcesFailed')}
              action={
                <Button
                  onClick={() => {
                    void centers.refetch()
                    void pipelines.refetch()
                  }}
                >
                  {t('common.retry')}
                </Button>
              }
            />
          )}
        </Form>
      )}
    </ActionDrawer>
  )
}
