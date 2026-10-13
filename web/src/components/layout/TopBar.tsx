import { KeyOutlined, LogoutOutlined } from '@ant-design/icons'
import { Avatar, Button, Dropdown, Space, Tag } from 'antd'
import type { MenuProps } from 'antd'
import { useTranslation } from 'react-i18next'
import { useNavigate } from 'react-router-dom'
import { useCurrentUser, useLogoutMutation } from '@/api/auth'
import { supportedLanguages, type SupportedLanguage } from '@/i18n'
import { enumLabel } from '@/utils/i18nEnum'

export function TopBar() {
  const { t, i18n } = useTranslation()
  const { user } = useCurrentUser()
  const logoutMutation = useLogoutMutation()
  const navigate = useNavigate()

  const languageItems: MenuProps['items'] = supportedLanguages.map((lang) => ({
    key: lang,
    label: lang,
  }))

  const handleLanguageChange: MenuProps['onClick'] = (info) => {
    void i18n.changeLanguage(info.key as SupportedLanguage)
  }

  return (
    <div
      className="app-top-bar"
      style={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'flex-end',
        height: '100%',
        paddingInline: 24,
        gap: 16,
      }}
    >
      <Dropdown menu={{ items: languageItems, onClick: handleLanguageChange }}>
        <Button type="text">{t('common.language')}</Button>
      </Dropdown>

      {user && (
        <Space>
          <Avatar size="small">{user.displayName.slice(0, 1)}</Avatar>
          <span className="top-bar-user-label">{user.displayName}</span>
          <Tag className="top-bar-user-label" color={user.role === 'admin' ? 'blue' : 'default'}>
            {/* role 来自后端 GET /auth/me，后端可新增角色。 */}
            {enumLabel(t, 'common.role', user.role)}
          </Tag>
        </Space>
      )}

      {user?.authSource === 'local' && (
        <Button
          type="text"
          aria-label={t('auth.changePassword')}
          icon={<KeyOutlined />}
          onClick={() => navigate('/change-password')}
        >
          <span className="top-bar-action-label">{t('auth.changePassword')}</span>
        </Button>
      )}
      <Button
        type="text"
        aria-label={t('common.logout')}
        icon={<LogoutOutlined />}
        loading={logoutMutation.isPending}
        onClick={() => logoutMutation.mutate()}
      >
        <span className="top-bar-action-label">{t('common.logout')}</span>
      </Button>
    </div>
  )
}
