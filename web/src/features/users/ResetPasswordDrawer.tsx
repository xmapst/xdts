import { ActionDrawer } from '@/components/ActionDrawer'
import { useHintFeedback } from '@/components/hintFeedback'
import { HintForm } from '@/components/HintForm'
import { useEffect } from 'react'
import { Form, Input } from 'antd'
import { useTranslation } from 'react-i18next'
import { useResetPasswordMutation, type UserSummary } from '@/api/users'

interface ResetPasswordFormValues {
  password: string
}

interface ResetPasswordDrawerProps {
  open: boolean
  onClose: () => void
  user?: UserSummary
}

/* Reset passwords through the separate endpoint for local accounts. Plaintext appears only in the request. */
export function ResetPasswordDrawer({ open, onClose, user }: ResetPasswordDrawerProps) {
  const { t } = useTranslation()
  const { message } = useHintFeedback()
  const [form] = Form.useForm<ResetPasswordFormValues>()
  const mutation = useResetPasswordMutation()

  useEffect(() => {
    if (open) {
      form.resetFields()
    }
  }, [open, form])

  const handleSubmit = (values: ResetPasswordFormValues) => {
    if (!user) {
      return
    }
    mutation.mutate(
      { id: user.id, input: { password: values.password } },
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
      open={open}
      title={t('users.resetPasswordTitle')}
      onClose={onClose}
      onConfirm={() => form.submit()}
      confirmLoading={mutation.isPending}
      confirmText={t('common.submit')}
      cancelText={t('common.cancel')}
      destroyOnHidden
    >
      <Form<ResetPasswordFormValues>
        form={form}
        name="userResetPasswordDrawer"
        layout="vertical"
        onFinish={handleSubmit}
        disabled={mutation.isPending}
      >
        <HintForm
          name="password"
          label={t('users.newPassword')}
          rules={[{ required: true, message: t('users.newPasswordRequired') }]}
        >
          <Input.Password autoComplete="new-password" />
        </HintForm>
      </Form>
    </ActionDrawer>
  )
}
