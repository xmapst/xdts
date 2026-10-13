import { HintAlert as Alert } from '@/components/HintAlert'
import { HintForm } from '@/components/HintForm'
import { Button, Form, Input, Typography } from 'antd'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { useTranslation } from 'react-i18next'
import { useNavigate } from 'react-router-dom'
import { authQueryKey, changePassword, useCurrentUser, useLogoutMutation } from '@/api/auth'

interface PasswordForm {
  oldPassword: string
  newPassword: string
  confirmPassword: string
}

export default function ChangePasswordPage() {
  const { t } = useTranslation()
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const { user } = useCurrentUser()
  const logout = useLogoutMutation()
  const change = useMutation({
    mutationFn: changePassword,
    meta: { suppressGlobalError: true },
    onSuccess: () => {
      queryClient.removeQueries({ queryKey: authQueryKey })
      navigate('/login', { replace: true })
    },
  })

  return (
    <main style={{ maxWidth: 400, margin: 'min(12vh, 96px) auto', padding: '0 20px' }}>
      <Typography.Title level={3}>
        <span className="section-title">
          {t('auth.changePassword')}{' '}
          {user?.mustChangePassword && (
            <Alert type="warning" showIcon title={t('auth.initialPasswordNotice')} />
          )}
          {change.error && <Alert type="error" showIcon title={change.error.message} />}
        </span>
      </Typography.Title>
      <Form<PasswordForm>
        layout="vertical"
        onFinish={({ oldPassword, newPassword }) =>
          change.mutate({ oldPassword, newPassword })
        }
        disabled={change.isPending}
      >
        <HintForm
          name="oldPassword"
          label={t('auth.oldPassword')}
          rules={[{ required: true, message: t('auth.passwordRequired') }]}
        >
          <Input.Password autoComplete="current-password" autoFocus />
        </HintForm>
        <HintForm
          name="newPassword"
          label={t('auth.newPassword')}
          rules={[
            { required: true, message: t('auth.passwordRequired') },
            {
              validator(_, value: string | undefined) {
                return !value || new TextEncoder().encode(value).length <= 1024
                  ? Promise.resolve()
                  : Promise.reject(new Error(t('auth.passwordTooLong')))
              },
            },
          ]}
        >
          <Input.Password autoComplete="new-password" />
        </HintForm>
        <HintForm
          name="confirmPassword"
          label={t('auth.confirmPassword')}
          dependencies={['newPassword']}
          rules={[
            { required: true, message: t('auth.passwordRequired') },
            ({ getFieldValue }) => ({
              validator(_, value: string) {
                return value === getFieldValue('newPassword')
                  ? Promise.resolve()
                  : Promise.reject(new Error(t('auth.passwordMismatch')))
              },
            }),
          ]}
        >
          <Input.Password autoComplete="new-password" />
        </HintForm>
        <div style={{ display: 'flex', gap: 8 }}>
          <Button type="primary" htmlType="submit" loading={change.isPending}>
            {t('auth.changePassword')}
          </Button>
          <Button onClick={() => logout.mutate()} loading={logout.isPending}>
            {t('common.logout')}
          </Button>
        </div>
      </Form>
    </main>
  )
}
