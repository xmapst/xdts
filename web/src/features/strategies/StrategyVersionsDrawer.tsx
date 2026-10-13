import { ScrollTable } from '@/components/ScrollTable'
import { useHintFeedback } from '@/components/hintFeedback'
import { HintIcon } from '@/components/HintIcon'
import { useState, useMemo } from 'react'
import { Button, Popconfirm, Space, Tag, Typography } from 'antd'
import { ActionDrawer } from '@/components/ActionDrawer'
import { PlusOutlined } from '@ant-design/icons'
import { useTranslation } from 'react-i18next'
import {
  createDefaultStrategyRules,
  useDeleteStrategyVersionMutation,
  useStrategyVersionsQuery,
  type StrategySummary,
  type StrategyVersionSummary,
} from '@/api/strategies'
import { RequireRole } from '@/components/RequireRole'
import { formatDateTime } from '@/utils/format'
import { StrategyRuleEditorDrawer } from './StrategyRuleEditorDrawer'
import { VersionDiffDrawer } from './VersionDiffDrawer'
import { fromWireRules } from '@/api/strategyRulesWire'

interface StrategyVersionsDrawerProps {
  open: boolean
  onClose: () => void
  strategy: StrategySummary | undefined
}

/* Hide current-version deletion and display server dependency errors. A strategy key resets retained drawer interaction state. */
export function StrategyVersionsDrawer({
  open,
  onClose,
  strategy,
}: StrategyVersionsDrawerProps) {
  const { t } = useTranslation()

  return (
    <ActionDrawer
      fillBody
      open={open}
      title={
        strategy
          ? `${t('strategies.versions.title')} · ${strategy.name}`
          : t('strategies.versions.title')
      }
      onClose={onClose}
      size={960}
      destroyOnHidden
    >
      {strategy && (
        <StrategyVersionsDrawerBody key={strategy.id} strategy={strategy} open={open} />
      )}
    </ActionDrawer>
  )
}

interface StrategyVersionsDrawerBodyProps {
  strategy: StrategySummary
  open: boolean
}

function StrategyVersionsDrawerBody({ strategy, open }: StrategyVersionsDrawerBodyProps) {
  const { t } = useTranslation()
  const { message } = useHintFeedback()
  const versionsQuery = useStrategyVersionsQuery(strategy.id, open)
  const deleteVersionMutation = useDeleteStrategyVersionMutation(strategy.id)

  const [editorOpen, setEditorOpen] = useState(false)
  const [selectedVersions, setSelectedVersions] = useState<number[]>([])
  const [diffOpen, setDiffOpen] = useState(false)

  const versions = versionsQuery.data ?? []
  const latest = versions.length > 0 ? versions[versions.length - 1] : undefined
  // `notices` 是"协议表达得出、本界面表达不出"的清单，必须一路传到编辑器：
  // 丢掉它等于把静默改写重新打开一次，而那恰恰是接这层映射要防的事。
  const inbound = useMemo(
    () => (latest ? fromWireRules(latest.rules, createDefaultStrategyRules()) : null),
    [latest],
  )
  const baseRules = inbound?.rules ?? createDefaultStrategyRules()
  const baseNotices = inbound?.notices ?? []

  const handleSelectChange = (keys: (string | number | bigint)[]) => {
    // 对比只需要两个版本；超过两个时保留最近选中的两个，而不是拒绝新的选择，
    // 减少用户需要先手动取消勾选的摩擦。
    const nums = keys.map(Number)
    setSelectedVersions(nums.length > 2 ? nums.slice(-2) : nums)
  }

  const handleDeleteVersion = (version: number) => {
    deleteVersionMutation.mutate(version, {
      onSuccess: () => void message.success(t('common.operationSucceeded')),
      // error.message 原样展示——见组件顶部注释，两个 409 错误码各自的文案
      // 已经点名拒绝原因，不用在这里按 code 再分支拼一遍提示。
      onError: (error) => void message.error(error.message),
    })
  }

  return (
    <>
      <Space style={{ marginBottom: 12, flexShrink: 0 }}>
        <RequireRole role="admin">
          <Button
            type="primary"
            icon={<PlusOutlined />}
            onClick={() => setEditorOpen(true)}
          >
            {t('strategies.versions.createVersion')}
          </Button>
        </RequireRole>
        <Button disabled={selectedVersions.length !== 2} onClick={() => setDiffOpen(true)}>
          {t('strategies.versions.compareSelected')}
        </Button>
        <HintIcon content={t('strategies.versions.compareHint')} />
      </Space>

      <ScrollTable<StrategyVersionSummary>
        rowKey="version"
        loading={versionsQuery.isLoading}
        dataSource={[...versions].sort((a, b) => b.version - a.version)}
        pagination={false}
        locale={{ emptyText: t('strategies.versions.empty') }}
        rowSelection={{
          selectedRowKeys: selectedVersions,
          onChange: handleSelectChange,
          type: 'checkbox',
        }}
        columns={[
          {
            title: t('strategies.versions.columns.version'),
            dataIndex: 'version',
            render: (value: number) => (
              <>
                v{value}
                {value === strategy.currentVersion && (
                  <Tag color="success" style={{ marginLeft: 8 }}>
                    {t('strategies.versions.current')}
                  </Tag>
                )}
              </>
            ),
          },
          { title: t('strategies.versions.columns.createdBy'), dataIndex: 'createdBy' },
          {
            title: t('strategies.versions.columns.createdAt'),
            dataIndex: 'createdAt',
            render: (value: string) => formatDateTime(value),
          },
          {
            title: t('common.actions'),
            key: 'actions',
            render: (_value, record) =>
              record.version === strategy.currentVersion ? null : (
                <RequireRole role="admin">
                  <Popconfirm
                    title={t('strategies.versions.deleteConfirm', {
                      version: record.version,
                    })}
                    onConfirm={() => handleDeleteVersion(record.version)}
                  >
                    {}
                    <Typography.Link type="danger">{t('common.delete')}</Typography.Link>
                  </Popconfirm>
                </RequireRole>
              ),
          },
        ]}
      />

      <StrategyRuleEditorDrawer
        open={editorOpen}
        onClose={() => setEditorOpen(false)}
        strategyId={strategy.id}
        baseRules={baseRules}
        baseNotices={baseNotices}
      />
      <VersionDiffDrawer
        open={diffOpen}
        onClose={() => setDiffOpen(false)}
        strategyId={strategy.id}
        versionA={selectedVersions[0]}
        versionB={selectedVersions[1]}
      />
    </>
  )
}
