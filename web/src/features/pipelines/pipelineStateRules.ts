import type { PipelineActualState, PipelineDesiredState, PipelineSide } from '@/api/pipelines'

/* Pipeline 状态规则：期望态与各侧实际态之间的纯判断。

分开之后还有一个与 lint 无关的好处：这三个函数是"后端会不会动"的前端镜像，
正确性由 `web/src/mocks/pipelineReconcileMirror.test.ts` 逐格对照后端 33 格对账表保证，
和"某个状态用什么颜色"根本不是一类东西。放在一起时，改配色的人和改收敛规则的
人会打开同一个文件。

不要在本文件里加任何 JSX 或组件——那会把 warning 原样带回来。 */

/* 某一侧的实际状态是否已经等于期望状态（字面相等）。

这是纯粹的字面比较，不含"该不该提示用户"的策略判断——那部分在
`divergentSides` 里，两者分开是因为"是否相等"是一个稳定的事实，而"该不该
因此提示用户"会随场景变化，混在一起会让后者的例外规则污染前者。 */
export function isPipelineStateConverged(
  desired: PipelineDesiredState,
  actual: PipelineActualState,
): boolean {
  return (actual as string) === (desired as string)
}

/** `divergentSides` 需要的最小输入，便于测试构造，也避免调用方必须凑一个完整
 * 的 PipelineSummary。 */
export interface PipelineSideStates {
  desiredState: PipelineDesiredState
  sourceActualState: PipelineActualState
  targetActualState: PipelineActualState
}

export function canResumePipeline(states: PipelineSideStates): boolean {
  return (
    states.desiredState === 'paused' &&
    states.sourceActualState === 'paused' &&
    states.targetActualState === 'paused'
  )
}

export function canPrecheckPipeline(states: PipelineSideStates): boolean {
  return (
    canResumePipeline(states) ||
    (states.desiredState === 'stopped' &&
      ['draft', 'stopped'].includes(states.sourceActualState) &&
      ['draft', 'stopped'].includes(states.targetActualState))
  )
}

/* A draft side needs a divergence hint once desired is running. Unknown sides still need a hint. */

export function actionRequiredSides(states: PipelineSideStates): PipelineSide[] {
  const sides: PipelineSide[] = []
  const check = (side: PipelineSide, actual: PipelineActualState) => {
    if (states.desiredState === 'running' && (actual === 'draft' || actual === 'prechecking')) {
      sides.push(side)
    }
  }
  check('source', states.sourceActualState)
  check('target', states.targetActualState)
  return sides
}

export function divergentSides(states: PipelineSideStates): PipelineSide[] {
  const sides: PipelineSide[] = []
  const actionRequired = new Set(actionRequiredSides(states))
  const check = (side: PipelineSide, actual: PipelineActualState) => {
    if (actual === 'error') {
      return
    }
    // 该侧已由 actionRequiredSides 的那条可操作提示覆盖——同一侧挂两条提示
    // 只会稀释那条真正要人动手的。与上面 error 那条是同一个排除理由：
    // 有更具体的提示时，笼统的那条让位。
    if (actionRequired.has(side)) {
      return
    }
    if (states.desiredState === 'stopped' && actual === 'draft') {
      return
    }
    // 这比"过渡期闪现"严重一档：闪现会自己消失，这一条不会。
    if (states.desiredState === 'paused' && actual === 'stopped') {
      return
    }
    if (!isPipelineStateConverged(states.desiredState, actual)) {
      sides.push(side)
    }
  }
  check('source', states.sourceActualState)
  check('target', states.targetActualState)
  return sides
}

export function canDeletePipeline(states: PipelineSideStates): boolean {
  return (
    states.desiredState === 'stopped' &&
    ['draft', 'stopped'].includes(states.sourceActualState) &&
    ['draft', 'stopped'].includes(states.targetActualState)
  )
}
