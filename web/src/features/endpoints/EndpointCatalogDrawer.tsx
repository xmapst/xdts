import { ScrollTable } from '@/components/ScrollTable'
import { HintAlert as Alert } from '@/components/HintAlert'
import { ActionDrawer } from '@/components/ActionDrawer'
import { CheckOutlined } from '@ant-design/icons'
import { useTranslation } from 'react-i18next'
import {
  useEndpointCatalogQuery,
  type CatalogEntry,
  type EndpointSummary,
} from '@/api/endpoints'

interface EndpointCatalogDrawerProps {
  open: boolean
  onClose: () => void
  endpoint: EndpointSummary | undefined
}

/** 浏览库表元数据（MGR-DS-04，P1），供策略配置与全量规划选择库表。 */
export function EndpointCatalogDrawer({
  open,
  onClose,
  endpoint,
}: EndpointCatalogDrawerProps) {
  const { t } = useTranslation()
  const catalogQuery = useEndpointCatalogQuery(endpoint?.id, open)

  return (
    <ActionDrawer
      fillBody
      open={open}
      title={<span className="section-title">{t('dataEndpoints.catalog.title')}</span>}
      onClose={onClose}
      size={960}
      destroyOnHidden
    >
      {catalogQuery.isError && <Alert type="error" title={catalogQuery.error.message} />}
      {!catalogQuery.isError && (
        <ScrollTable<CatalogEntry>
          rowKey={(row) => `${row.schema}.${row.table}`}
          loading={catalogQuery.isLoading}
          dataSource={catalogQuery.data ?? []}
          pagination={false}
          columns={[
            { title: t('dataEndpoints.catalog.columns.schema'), dataIndex: 'schema' },
            { title: t('dataEndpoints.catalog.columns.table'), dataIndex: 'table' },
            {
              title: t('dataEndpoints.catalog.columns.estimatedRows'),
              dataIndex: 'estimatedRows',
              render: (value: number | null) =>
                value === null ? '—' : value.toLocaleString(),
            },
            {
              title: t('dataEndpoints.catalog.columns.primaryKey'),
              dataIndex: 'hasPrimaryKey',
              render: (value: boolean) =>
                value ? <CheckOutlined style={{ color: '#52c41a' }} /> : '—',
            },
            {
              title: t('dataEndpoints.catalog.columns.uniqueKey'),
              dataIndex: 'hasUniqueKey',
              render: (value: boolean) =>
                value ? <CheckOutlined style={{ color: '#52c41a' }} /> : '—',
            },
          ]}
          locale={{ emptyText: t('dataEndpoints.catalog.empty') }}
        />
      )}
    </ActionDrawer>
  )
}
