import type { PipelineActualState } from '@/api/pipelines'

/**
 * 某一侧要不要重启才会用上新的端点连接信息。
 *
 * 三态而不是布尔：`unknown` 是"该侧所在节点离线，后端记为未知、不改写"
 * （见 web/src/api/pipelines.ts 的 PipelineActualState 注释），它既不是"在跑"也不是
 * "已停"。压成布尔就必须在两个方向上挑一个撒谎，而两个方向的代价不对称：
 * 说成"不用重启"会让运维漏掉一侧，那正是 MGR-DS-05 这条提示要防的事。
 */
export type SideRestartNeed = 'needed' | 'not_needed' | 'unknown'

export function sideRestartNeed(state: PipelineActualState): SideRestartNeed {
  if (state === 'stopped') {
    return 'not_needed'
  }
  if (state === 'unknown') {
    return 'unknown'
  }
  return 'needed'
}
