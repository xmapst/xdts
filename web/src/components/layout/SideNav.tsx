import { ConfigProvider, Menu, Tooltip } from 'antd'
import type { MenuProps } from 'antd'
import { useMemo } from 'react'
import { useTranslation } from 'react-i18next'
import { useLocation, useNavigate } from 'react-router-dom'
import { useCurrentUser } from '@/api/auth'
import { navGroupOrder, navItems } from './navConfig'

const GROUP_KEY_PREFIX = 'group:'

export function SideNav({ collapsed = false }: { collapsed?: boolean }) {
  const { t } = useTranslation()
  const navigate = useNavigate()
  const location = useLocation()
  const { role } = useCurrentUser()

  const items: MenuProps['items'] = useMemo(() => {
    const visibleItems = navItems.filter(
      (item) => !item.roles || (role && item.roles.includes(role)),
    )
    const toMenuItem = (item: (typeof visibleItems)[number]) => ({
      key: item.path,
      icon: item.icon,
      label: t(`nav.${item.labelKey}`),
    })

    const ungroupedMenuItems = visibleItems.filter((item) => !item.group).map(toMenuItem)
    const groupedMenuItems: MenuProps['items'] = navGroupOrder
      .map((group) => {
        const children = visibleItems.filter((item) => item.group === group.key).map(toMenuItem)
        if (children.length === 0) {
          return null
        }
        return {
          type: 'group' as const,
          key: `${GROUP_KEY_PREFIX}${group.key}`,
          label: collapsed ? (
            <Tooltip title={t(`nav.groups.${group.labelKey}`)} placement="right">
              <span
                className="side-nav-group-label"
                role="img"
                aria-label={t(`nav.groups.${group.labelKey}`)}
                tabIndex={0}
              >
                {group.icon}
              </span>
            </Tooltip>
          ) : (
            <span className="side-nav-group-label">
              {group.icon}
              {t(`nav.groups.${group.labelKey}`)}
            </span>
          ),
          children,
        }
      })
      .filter((group): group is NonNullable<typeof group> => group !== null)

    return [...ungroupedMenuItems, ...groupedMenuItems]
  }, [collapsed, role, t])

  const selectedKey = useMemo(() => {
    const match = navItems.find(
      (item) =>
        location.pathname === item.path || location.pathname.startsWith(`${item.path}/`),
    )
    return match ? [match.path] : []
  }, [location.pathname])

  const handleClick: MenuProps['onClick'] = (info) => {
    navigate(info.key)
  }

  return (
    <ConfigProvider theme={{ components: { Menu: { itemHeight: 32, itemMarginBlock: 2 } } }}>
      <Menu
        mode="inline"
        inlineCollapsed={collapsed}
        theme="dark"
        selectedKeys={selectedKey}
        items={items}
        onClick={handleClick}
        style={{ borderInlineEnd: 'none' }}
      />
    </ConfigProvider>
  )
}
