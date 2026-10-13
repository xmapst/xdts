import type { TFunction } from 'i18next'
import { enumLabel } from '@/utils/i18nEnum'
import { formatDateTime } from '@/utils/format'

/* 事件确认状态。四态不是三态，因为 `ackedAt` 的 `undefined` 与 `null`
含义不同（见 `web/src/api/events.ts` 的 `EventEntry.ackedAt`）：

最后两态在界面上必须不同：`acked_time_unknown` 要明说"时间不详"，因为
那一行的时间确实不可知；而 `acked_field_absent` 时整个字段都不存在，说
"时间不详"等于在陈述一个这个后端根本没有的概念，只显示"已确认"。 */
export type AckState =
  | { kind: 'unacked' }
  | { kind: 'acked'; at: string }
  | { kind: 'acked_time_unknown' }
  | { kind: 'acked_field_absent' }

export function ackState(ackedBy: string | null, ackedAt: string | null | undefined): AckState {
  if (ackedBy === null) {
    return { kind: 'unacked' }
  }
  if (ackedAt === undefined) {
    return { kind: 'acked_field_absent' }
  }
  if (ackedAt === null) {
    return { kind: 'acked_time_unknown' }
  }
  return { kind: 'acked', at: ackedAt }
}

/** 确认状态的显示文案（列渲染与 CSV 导出共用，两处不得各写一份）。 */
export function ackStateText(state: AckState, t: TFunction): string {
  switch (state.kind) {
    case 'unacked':
      return t('events.ackedNo')
    case 'acked':
      return `${t('events.ackedYes')} ${formatDateTime(state.at)}`
    case 'acked_time_unknown':
      return t('events.ackedTimeUnknown')
    case 'acked_field_absent':
      return t('events.ackedYes')
  }
}

export function eventTypeLabel(value: string, t: TFunction): string {
  return enumLabel(t, 'events.types', value)
}

export function ackErrorMessageKey(code: string | undefined): {
  key: 'events.ackErrorAlreadyAcked' | 'events.ackErrorNotFound' | 'events.ackErrorFailed'
  level: 'warning' | 'error'
} {
  switch (code) {
    case 'event_already_acked':
      return { key: 'events.ackErrorAlreadyAcked', level: 'warning' }
    case 'not_found':
      return { key: 'events.ackErrorNotFound', level: 'warning' }
    default:
      // 400 invalid_id 与三种 500 都落这里：它们都不是用户能处置的情况，
      // 区分文案只会让用户面对一个他做不了任何事的错误码。
      return { key: 'events.ackErrorFailed', level: 'error' }
  }
}
