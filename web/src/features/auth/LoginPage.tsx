import { HintAlert as Alert } from '@/components/HintAlert'
import { HintForm } from '@/components/HintForm'
import { HintIcon } from '@/components/HintIcon'
import { Button, Card, Divider, Form, Input, Typography } from 'antd'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { useTranslation } from 'react-i18next'
import { Navigate, useLocation, useNavigate } from 'react-router-dom'
import {
  OIDC_START_PATH,
  authQueryKey,
  mockOidcLogin,
  useCurrentUser,
  useLoginMutation,
} from '@/api/auth'
import { isApiMockEnabled } from '@/mocks/config'

interface LoginFormValues {
  username: string
  password: string
}

const DEFAULT_REDIRECT = '/dashboard'

/** 从 react-router 的 location.state 中取出登录前意图访问的路径，无则回退到仪表盘。 */
function resolveRedirectTarget(state: unknown): string {
  if (typeof state !== 'object' || state === null) {
    return DEFAULT_REDIRECT
  }
  const from = (state as { from?: unknown }).from
  if (typeof from !== 'object' || from === null) {
    return DEFAULT_REDIRECT
  }
  const pathname = (from as { pathname?: unknown }).pathname
  return typeof pathname === 'string' && pathname.length > 0 ? pathname : DEFAULT_REDIRECT
}

/**
 * 登录页：本地账号 / LDAP 共用同一表单（后端按配置路由），OIDC 单独跳转。
 * 哪些方式可用由 useCurrentUser() 的 methods 决定（见 web/src/api/auth.ts 顶部注释
 * 关于 GET /auth/me 契约的补充说明）。
 */
export default function LoginPage() {
  const { t } = useTranslation()
  const navigate = useNavigate()
  const location = useLocation()
  const queryClient = useQueryClient()
  const { isLoading, isAuthenticated, methods, user } = useCurrentUser()
  const loginMutation = useLoginMutation()
  const oidcMockMutation = useMutation({
    mutationFn: mockOidcLogin,
    meta: { suppressGlobalError: true },
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: authQueryKey })
      navigate(resolveRedirectTarget(location.state), { replace: true })
    },
  })

  if (isAuthenticated) {
    if (user?.mustChangePassword) return <Navigate to="/change-password" replace />
    return <Navigate to={resolveRedirectTarget(location.state)} replace />
  }

  const showPasswordForm = Boolean(methods?.local) || Boolean(methods?.ldap)
  const showOidc = Boolean(methods?.oidc)
  const mockEnabled = isApiMockEnabled()

  const handleSubmit = (values: LoginFormValues) => {
    loginMutation.mutate(values, {
      onSuccess: (data) => {
        navigate(
          data.user.mustChangePassword
            ? '/change-password'
            : resolveRedirectTarget(location.state),
          { replace: true },
        )
      },
    })
  }

  const handleOidcClick = () => {
    if (mockEnabled) {
      oidcMockMutation.mutate()
      return
    }
    window.location.href = OIDC_START_PATH
  }

  return (
    <div
      style={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        minHeight: '100vh',
        background: '#f0f2f5',
      }}
    >
      <Card style={{ width: 380 }} loading={isLoading}>
        <Typography.Title level={3} style={{ textAlign: 'center', marginTop: 0 }}>
          <span className="section-title">
            {t('auth.loginTitle')}{' '}
            {mockEnabled && <Alert type="info" showIcon title={t('auth.mockHint')} />}
            {loginMutation.error && (
              <Alert
                type="error"
                showIcon
                title={t('auth.loginFailed')}
                description={loginMutation.error.message}
              />
            )}
            {oidcMockMutation.error && (
              <Alert
                type="error"
                showIcon
                title={t('auth.loginFailed')}
                description={oidcMockMutation.error.message}
              />
            )}
            {!isLoading && !showPasswordForm && !showOidc && (
              <Alert type="warning" showIcon title={t('auth.noMethodsAvailable')} />
            )}
          </span>
        </Typography.Title>

        {showPasswordForm && (
          <Form<LoginFormValues>
            layout="vertical"
            onFinish={handleSubmit}
            disabled={loginMutation.isPending}
          >
            <HintForm
              name="username"
              label={
                <span className="field-label-hint">
                  {t('auth.username')}
                  <HintIcon content={t('auth.loginLocalOrLdapHint')} />
                </span>
              }
              rules={[{ required: true, message: t('auth.usernameRequired') }]}
            >
              <Input autoComplete="username" autoFocus />
            </HintForm>
            <HintForm
              name="password"
              label={t('auth.password')}
              rules={[{ required: true, message: t('auth.passwordRequired') }]}
            >
              <Input.Password autoComplete="current-password" />
            </HintForm>
            <HintForm style={{ marginBottom: showOidc ? 0 : undefined }}>
              <Button
                type="primary"
                htmlType="submit"
                block
                loading={loginMutation.isPending}
              >
                {t('auth.loginSubmit')}
              </Button>
            </HintForm>
          </Form>
        )}

        {showPasswordForm && showOidc && <Divider>{t('auth.orDivider')}</Divider>}

        {showOidc && (
          <Button block onClick={handleOidcClick} loading={oidcMockMutation.isPending}>
            {t('auth.oidcLogin')}
          </Button>
        )}
      </Card>
    </div>
  )
}
