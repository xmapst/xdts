import type { TFunction } from 'i18next'
import { enumLabel } from '@/utils/i18nEnum'

/* Known local init modes use typed translation keys. Values received from the server need a raw-value fallback. */
export function initModeLabel(value: string, t: TFunction): string {
  return enumLabel(t, 'pipelines.initMode', value)
}

/* Use shared label functions for tags and CSV exports so unknown server values use the same fallback. */
export function desiredStateLabel(value: string, t: TFunction): string {
  return enumLabel(t, 'pipelines.desiredState', value)
}

export function actualStateLabel(value: string, t: TFunction): string {
  return enumLabel(t, 'pipelines.actualState', value)
}

export function releaseStatusLabel(value: string, t: TFunction): string {
  return enumLabel(t, 'pipelines.releaseStatus', value)
}
