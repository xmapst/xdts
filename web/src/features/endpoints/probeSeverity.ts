// 探测结果的参数达标判定：纯函数，不依赖组件生命周期，方便单测。
// 判定依据： 实例参数要求
// （阻断 / 推荐 / 警告 / 提示 四级）。
import type { ProbeResult } from '@/api/endpoints'

export type ParamSeverity = 'block' | 'recommend' | 'warn' | 'info'

export interface ParamCheck {
  key: string
  /** i18n key 后缀，对应 `dataEndpoints.probe.params.<paramKey>`。 */
  paramKey: string
  level: ParamSeverity
  actual: string
  /**
   * true=达标，false=不达标，null=无法仅凭探测结果自动判定（如
   * binlog_expire_logs_seconds 需要结合全量预计耗时，属于部署规范类判断）。
   */
  ok: boolean | null
}

const NONE_DISPLAY = '—'

function display(value: string | number | null): string {
  if (value === null || value === '') {
    return NONE_DISPLAY
  }
  return String(value)
}

/**
 * 把一次探测结果拆成 表格里的逐项检查。只在 `reachable` 为 true 时有
 * 意义——连接失败时调用方应优先展示 `error`，不必渲染这张表。
 */
export function evaluateProbeParams(result: ProbeResult): ParamCheck[] {
  return [
    {
      key: 'gtid',
      paramKey: 'gtidMode',
      level: 'block',
      actual: `${display(result.gtidMode)} / ${display(result.enforceGtidConsistency)}`,
      ok:
        result.gtidMode === null || result.enforceGtidConsistency === null
          ? null
          : result.gtidMode === 'ON' && result.enforceGtidConsistency === 'ON',
    },
    {
      key: 'logBin',
      paramKey: 'logBin',
      level: 'block',
      actual: display(result.logBin),
      ok: result.logBin === null ? null : result.logBin === 'ON',
    },
    {
      key: 'binlogFormat',
      paramKey: 'binlogFormat',
      level: 'block',
      actual: display(result.binlogFormat),
      ok: result.binlogFormat === null ? null : result.binlogFormat === 'ROW',
    },
    {
      key: 'binlogRowImage',
      paramKey: 'binlogRowImage',
      level: 'block',
      actual: display(result.binlogRowImage),
      ok: result.binlogRowImage === null ? null : result.binlogRowImage === 'FULL',
    },
    {
      key: 'serverUuid',
      paramKey: 'serverUuid',
      level: 'block',
      actual: display(result.serverUuid),
      // Presence is checked here; the server registers identities and rejects cross-endpoint conflicts.
      ok: Boolean(result.serverUuid),
    },
    {
      key: 'binlogRowMetadata',
      paramKey: 'binlogRowMetadata',
      level: 'recommend',
      actual: display(result.binlogRowMetadata),
      ok: result.binlogRowMetadata === null ? null : result.binlogRowMetadata === 'FULL',
    },
    {
      key: 'binlogExpireLogsSeconds',
      paramKey: 'binlogExpireLogsSeconds',
      level: 'warn',
      actual: display(result.binlogExpireLogsSeconds),
      // 要求是「≥ max(全量预计耗时, 可容忍最长中断时间)」，脱离具体 pipeline
      // 规划无法自动判定，只展示数值供人工核对。
      ok: null,
    },
    {
      key: 'binlogRowValueOptions',
      paramKey: 'binlogRowValueOptions',
      level: 'info',
      actual: display(result.binlogRowValueOptions),
      ok: result.binlogRowValueOptions === null ? null : !result.binlogRowValueOptions,
    },
    {
      key: 'binlogTransactionCompression',
      paramKey: 'binlogTransactionCompression',
      level: 'info',
      actual: display(result.binlogTransactionCompression),
      ok: null,
    },
  ]
}
