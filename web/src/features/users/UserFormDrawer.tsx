import { ActionDrawer } from '@/components/ActionDrawer'
import { useHintFeedback } from '@/components/hintFeedback'
import { HintForm } from '@/components/HintForm'
import { useEffect } from 'react'
import { Form, Input, Select } from 'antd'
import { useTranslation } from 'react-i18next'
import type { UserRole } from '@/api/auth'
import { useCurrentUser } from '@/api/auth'
import { useCreateUserMutation, useUpdateUserMutation, type UserSummary } from '@/api/users'

interface UserFormValues {
  username: string
  /** 仅新建时使用；编辑模式下这个字段根本不出现在表单里（密码重置是契约
   *  的独立端点/独立弹窗 web/src/features/users/ResetPasswordDrawer.tsx，不混进这个表单）。 */
  password?: string
  role: UserRole
}

interface UserFormDrawerProps {
  open: boolean
  onClose: () => void
  /** 传入表示编辑；不传表示新建。 */
  user?: UserSummary
}

const ROLES: UserRole[] = ['admin', 'readonly']

/* Keep external usernames visible but read-only. Disable role changes when editing the current administrator. */
export function UserFormDrawer({ open, onClose, user }: UserFormDrawerProps) {
  const { t } = useTranslation()
  const { message } = useHintFeedback()
  const { user: currentUser } = useCurrentUser()
  const [form] = Form.useForm<UserFormValues>()
  const createMutation = useCreateUserMutation()
  const updateMutation = useUpdateUserMutation()
  const isEdit = Boolean(user)
  const isExternal = isEdit && user?.authSource !== 'local'
  const isSelf = isEdit && Boolean(currentUser) && user?.id === currentUser?.id
  const pending = createMutation.isPending || updateMutation.isPending

  useEffect(() => {
    if (!open) {
      return
    }
    form.setFieldsValue({
      username: user?.username ?? '',
      role: user?.role,
      password: undefined,
    })
  }, [open, user, form])

  const handleSubmit = (values: UserFormValues) => {
    const onSettled = {
      onSuccess: () => {
        void message.success(t('common.operationSucceeded'))
        onClose()
      },
      onError: (error: Error) => void message.error(error.message),
    }
    if (isEdit && user) {
      // 外部账号不提交 username 字段——即使输入框里显示的值和原值一样，
      // "请求体里带没带这个字段"本身就是部分更新的判定依据，不是"值变没变"，
      // 对外部账号必须完全不带这个键，不能带一个"没变化"的同值。
      updateMutation.mutate(
        {
          id: user.id,
          input: isExternal
            ? { role: values.role }
            : { username: values.username, role: values.role },
        },
        onSettled,
      )
    } else {
      createMutation.mutate(
        { username: values.username, password: values.password ?? '', role: values.role },
        onSettled,
      )
    }
  }

  return (
    <ActionDrawer
      open={open}
      title={t(isEdit ? 'users.edit' : 'users.create')}
      onClose={onClose}
      onConfirm={() => form.submit()}
      confirmLoading={pending}
      confirmText={t('common.submit')}
      cancelText={t('common.cancel')}
      destroyOnHidden
    >
      <Form<UserFormValues>
        form={form}
        // 显式 name：避免字段 id 与本页面同屏可能出现的其它 Form 裸 id 相撞，
        name="userFormDrawer"
        layout="vertical"
        onFinish={handleSubmit}
        disabled={pending}
      >
        <HintForm
          name="username"
          label={t('users.form.username')}
          rules={
            isExternal
              ? []
              : [{ required: true, message: t('users.form.usernameRequired') }]
          }
          extra={isExternal ? t('users.form.usernameExternalHint') : undefined}
        >
          <Input disabled={isExternal} />
        </HintForm>
        {!isEdit && (
          <HintForm
            name="password"
            label={t('users.form.password')}
            rules={[{ required: true, message: t('users.form.passwordRequired') }]}
          >
            <Input.Password autoComplete="new-password" />
          </HintForm>
        )}
        <HintForm
          name="role"
          label={t('users.form.role')}
          rules={[{ required: true, message: t('users.form.roleRequired') }]}
          extra={isSelf ? t('users.form.roleSelfHint') : undefined}
        >
          <Select
            disabled={isSelf}
            options={ROLES.map((role) => ({
              value: role,
              label: t(`common.role.${role}`),
            }))}
          />
        </HintForm>
      </Form>
    </ActionDrawer>
  )
}
