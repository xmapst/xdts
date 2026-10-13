import { ActionDrawer } from '@/components/ActionDrawer'
import { ScrollTable } from '@/components/ScrollTable'
import { HintIcon } from '@/components/HintIcon'
import { Tag, Typography } from 'antd'
import { useTranslation } from 'react-i18next'
import {
  useStrategyVersionDiffQuery,
  type StrategyDiffChangeType,
  type StrategyDiffEntry,
} from '@/api/strategies'

interface VersionDiffDrawerProps {
  open: boolean
  onClose: () => void
  strategyId: string
  versionA: number | undefined
  versionB: number | undefined
}

const CHANGE_COLOR: Record<StrategyDiffChangeType, string> = {
  added: 'success',
  removed: 'error',
  changed: 'processing',
}

function formatValue(value: unknown): string {
  if (value === undefined) {
    return '—'
  }
  if (typeof value === 'string') {
    return value || '""'
  }
  return JSON.stringify(value)
}

/** 版本对比（MGR-STR-01）：始终把较小的版本号当作旧值（`base`）、较大的当作
 * 新值（`version`）展示，不管用户在表格里先勾选了哪一个，避免「旧值/新值」
 * 方向和直觉相反——这恰好对应后端 `?base=<version>` 契约的 `base → version`
 * 方向（见 web/src/api/strategies.ts 文件头注释 4）。 */
export function VersionDiffDrawer({
  open,
  onClose,
  strategyId,
  versionA,
  versionB,
}: VersionDiffDrawerProps) {
  const { t } = useTranslation()
  const [base, version] =
    versionA !== undefined && versionB !== undefined
      ? [Math.min(versionA, versionB), Math.max(versionA, versionB)]
      : [undefined, undefined]
  const diffQuery = useStrategyVersionDiffQuery(strategyId, version, base)

  return (
    <ActionDrawer
      fillBody
      open={open}
      title={
        <span className="section-title">
          {base !== undefined && version !== undefined
            ? `${t('strategies.diff.title')} v${base} → v${version}`
            : t('strategies.diff.title')}
        </span>
      }
      onClose={onClose}
      footer={null}
      size={960}
      destroyOnHidden
    >
      {diffQuery.isError && <HintIcon type="error" content={diffQuery.error.message} />}
      <ScrollTable<StrategyDiffEntry>
        rowKey="path"
        size="small"
        loading={diffQuery.isLoading}
        dataSource={diffQuery.data ?? []}
        pagination={false}
        locale={{ emptyText: t('strategies.diff.empty') }}
        columns={[
          { title: t('strategies.diff.path'), dataIndex: 'path' },
          {
            title: '',
            key: 'changeType',
            dataIndex: 'changeType',
            width: 90,
            render: (value: StrategyDiffChangeType) => (
              <Tag color={CHANGE_COLOR[value]}>
                {t(`strategies.diff.changeType.${value}`)}
              </Tag>
            ),
          },
          {
            title: t('strategies.diff.oldValue'),
            dataIndex: 'oldValue',
            render: (value: unknown) => (
              <Typography.Text code>{formatValue(value)}</Typography.Text>
            ),
          },
          {
            title: t('strategies.diff.newValue'),
            dataIndex: 'newValue',
            render: (value: unknown) => (
              <Typography.Text code>{formatValue(value)}</Typography.Text>
            ),
          },
        ]}
      />
    </ActionDrawer>
  )
}
