import { Tag } from 'antd'
import { useTranslation } from 'react-i18next'
import type {
  PipelineActualState,
  PipelineDesiredState,
  PipelineReleaseStatus,
} from '@/api/pipelines'
import { actualStateLabel, desiredStateLabel, releaseStatusLabel } from './pipelineLabels'

const DESIRED_STATE_COLOR: Record<PipelineDesiredState, string> = {
  running: 'success',
  paused: 'warning',
  stopped: 'default',
}

/** 期望状态标签：列表与详情页共用。desiredState 是用户/系统提交的目标，不是
 * "已经发生的事实"——颜色语义上和 actualState 保持独立映射，不共用一份表，
 * 避免"期望运行但实际未知"这类组合被误读成同一件事（同节点 desired* /
 * actual* 日志级别拆分展示的原则）。 */
export function PipelineDesiredStateTag({ state }: { state: PipelineDesiredState }) {
  const { t } = useTranslation()
  return <Tag color={DESIRED_STATE_COLOR[state]}>{desiredStateLabel(state, t)}</Tag>
}

const ACTUAL_STATE_COLOR: Record<PipelineActualState, string> = {
  draft: 'default',
  prechecking: 'processing',
  deploying: 'processing',
  migrating: 'processing',
  snapshotting: 'processing',
  catching_up: 'processing',
  running: 'success',
  paused: 'warning',
  error: 'error',
  stopped: 'default',

  unknown: 'warning',
}

/** 实际状态标签：列表与详情页共用。 */
export function PipelineActualStateTag({ state }: { state: PipelineActualState }) {
  const { t } = useTranslation()
  return <Tag color={ACTUAL_STATE_COLOR[state]}>{actualStateLabel(state, t)}</Tag>
}

const RELEASE_STATUS_COLOR: Record<PipelineReleaseStatus, string> = {
  pending: 'processing',
  applied: 'success',
  failed: 'error',
  // rejected 表示版本倒退被安全机制拒绝，严重性低于执行失败。
  rejected: 'warning',
}

export function PipelineReleaseStatusTag({ status }: { status: PipelineReleaseStatus }) {
  const { t } = useTranslation()
  return <Tag color={RELEASE_STATUS_COLOR[status]}>{releaseStatusLabel(status, t)}</Tag>
}

