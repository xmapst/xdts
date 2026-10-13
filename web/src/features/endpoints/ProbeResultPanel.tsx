import { ScrollTable } from '@/components/ScrollTable'
import { HintAlert as Alert } from '@/components/HintAlert'
import { Tag, Typography } from 'antd'
import {
  CheckCircleOutlined,
  CloseCircleOutlined,
  QuestionCircleOutlined,
} from '@ant-design/icons'
import { useTranslation } from 'react-i18next'
import type { ProbeResult } from '@/api/endpoints'
import { formatDateTime } from '@/utils/format'
import { useNow } from '@/utils/useNow'
import { evaluateProbeParams, type ParamCheck, type ParamSeverity } from './probeSeverity'
import { enumLabel } from '@/utils/i18nEnum'

const SEVERITY_COLOR: Record<ParamSeverity, string> = {
  block: 'red',
  recommend: 'orange',
  warn: 'gold',
  info: 'blue',
}

/** 探测结果距现在超过该时长时标记为「可能已过期」，纯前端展示层面的经验值，
 * 不是后端契约的一部分（只要求「标注采集时间」，具体新鲜度阈值
 * 由界面自行决定）。 */
const STALE_THRESHOLD_MS = 3600_000

interface ProbeResultPanelProps {
  lastProbeAt: string | null
  result: ProbeResult | null
}

/** 展示一次探测结果：采集时间 / 新鲜度、连通性、 参数达标表、账号权限。
 * 列表页「最近探测」列与探测弹窗的结果区共用同一个组件。 */
export function ProbeResultPanel({ lastProbeAt, result }: ProbeResultPanelProps) {
  const { t } = useTranslation()
  // 每分钟刷新一次即可：这只影响「是否过期」标签的展示，不需要秒级精度。
  const now = useNow(60_000)

  if (!lastProbeAt || !result) {
    return (
      <Typography.Text type="secondary">
        {t('dataEndpoints.freshness.never')}
      </Typography.Text>
    )
  }

  const isStale = now - new Date(lastProbeAt).getTime() > STALE_THRESHOLD_MS
  const checks = result.reachable ? evaluateProbeParams(result) : []

  return (
    <div>
      <Typography.Paragraph>
        {t('dataEndpoints.probe.collectedAt')}：{formatDateTime(lastProbeAt)}
        <Alert
          type={result.reachable ? 'success' : 'error'}
          title={t(
            result.reachable
              ? 'dataEndpoints.probe.reachable'
              : 'dataEndpoints.probe.unreachable',
          )}
          description={
            !result.reachable ? (result.error ?? t('common.unknownError')) : undefined
          }
        />
        {isStale && (
          <Tag color="warning" style={{ marginLeft: 8 }}>
            {t('dataEndpoints.freshness.staleSuffix')}
          </Tag>
        )}
      </Typography.Paragraph>

      {result.reachable && (
        <>
          <Typography.Title level={5}>
            {t('dataEndpoints.probe.resultTitle')}
          </Typography.Title>
          <ScrollTable<ParamCheck>
            size="small"
            rowKey="key"
            pagination={false}
            dataSource={checks}
            columns={[
              {
                title: t('dataEndpoints.probe.columns.level'),
                key: 'level',
                dataIndex: 'level',
                width: 96,
                render: (level: ParamSeverity) => (
                  <Tag color={SEVERITY_COLOR[level]}>
                    {t(`dataEndpoints.probe.severity.${level}`)}
                  </Tag>
                ),
              },
              {
                title: t('dataEndpoints.probe.columns.param'),
                key: 'param',
                dataIndex: 'paramKey',
                // paramKey 类型上就是 string，来自后端探测结果，前端拿不到它的
                // 值域——后端多探一个参数，这里就会显示一串
                // `dataEndpoints.probe.params.xxx`。走 enumLabel 兜底成后端原值。
                render: (paramKey: string) =>
                  enumLabel(t, 'dataEndpoints.probe.params', paramKey),
              },
              {
                title: t('dataEndpoints.probe.columns.actual'),
                key: 'actual',
                dataIndex: 'actual',
              },
              {
                title: t('dataEndpoints.probe.columns.ok'),
                key: 'ok',
                dataIndex: 'ok',
                width: 48,
                align: 'center',
                render: (ok: boolean | null) => {
                  if (ok === null) {
                    return <QuestionCircleOutlined style={{ color: '#faad14' }} />
                  }
                  return ok ? (
                    <CheckCircleOutlined style={{ color: '#52c41a' }} />
                  ) : (
                    <CloseCircleOutlined style={{ color: '#ff4d4f' }} />
                  )
                },
              },
            ]}
          />
          {result.grants.length > 0 && (
            <>
              <Typography.Title level={5} style={{ marginTop: 16 }}>
                {t('dataEndpoints.probe.grantsTitle')}
              </Typography.Title>
              <Typography.Paragraph>
                <ul style={{ margin: 0, paddingLeft: 20 }}>
                  {result.grants.map((grant) => (
                    <li key={grant} style={{ fontFamily: 'monospace', fontSize: 12 }}>
                      {grant}
                    </li>
                  ))}
                </ul>
              </Typography.Paragraph>
            </>
          )}
        </>
      )}
    </div>
  )
}
