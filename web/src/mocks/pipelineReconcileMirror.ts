import type {
  PipelineActualState,
  PipelineDesiredState,
  PipelineSide,
  PipelineSummary,
} from '@/api/pipelines'

/** 后端 `iproto.PipelineCmdType` 在 mock 侧的最小镜像。 */
export type MirroredPipelineCmd = 'start' | 'pause' | 'resume' | 'stop'

/* `converged` 与 `manual_gate` 在界面上都是「不动」，含义却相反：一个是
做完了，一个是卡住了。这正是本类型存在的全部理由——把它们合成一个
「未收敛」布尔值，前端就永远测不出把两者画成同一个样子的缺陷。 */
export type SideStandstillReason =
  | 'converged'
  | 'self_advancing'
  | 'manual_gate'
  | 'missing_edge'
  | 'offline'

export type SideReconcileOutcome =
  | { readonly kind: 'command'; readonly cmd: MirroredPipelineCmd }
  | { readonly kind: 'standstill'; readonly reason: SideStandstillReason }

const cmd = (c: MirroredPipelineCmd): SideReconcileOutcome => ({ kind: 'command', cmd: c })
const still = (r: SideStandstillReason): SideReconcileOutcome => ({ kind: 'standstill', reason: r })

/* The nested Record requires an entry for every desired-state and side actual-state pair. */
export const MIRRORED_RECONCILE_TABLE: Record<
  PipelineDesiredState,
  Record<PipelineActualState, SideReconcileOutcome>
> = {
  running: {
    // 预检查的唯一出边「通过并启动」是用户在向导里点的，对账循环没有任何
    // 指令可以表达「开始预检查」或「替用户确认」。
    draft: still('manual_gate'),
    prechecking: still('manual_gate'),
    // 状态图上没有由 Manager 指令触发的出边；成功或失败都由节点自行上报。
    deploying: still('self_advancing'),
    migrating: still('self_advancing'),
    // 方向本来就一致：快照完 → CatchingUp → Running，不需要纠正。
    snapshotting: still('self_advancing'),
    catching_up: still('self_advancing'),
    running: still('converged'),
    paused: cmd('resume'),
    error: cmd('resume'),
    stopped: cmd('start'),
    // MGR-PL-16「节点离线不重发指令」。
    unknown: still('offline'),
  },
  paused: {
    // 从未启动过的 pipeline 既没在跑、也就无从暂停。
    draft: still('manual_gate'),
    prechecking: still('manual_gate'),
    deploying: still('self_advancing'),
    migrating: still('self_advancing'),
    // **这一格是 send，不是 skip**——desired=paused 下唯一一个「进行中却要
    // 立刻发指令」的格子。写这张表时最容易顺手抄成 self_advancing。
    snapshotting: cmd('pause'),
    catching_up: still('self_advancing'),
    running: cmd('pause'),
    paused: still('converged'),
    error: cmd('pause'),

    stopped: still('converged'),
    unknown: still('offline'),
  },
  stopped: {
    // 刚创建、从未启动过：desired=stopped 与 actual=draft 语义上已经一致。
    draft: still('converged'),
    prechecking: still('manual_gate'),
    deploying: still('self_advancing'),
    migrating: still('self_advancing'),
    // Snapshotting/CatchingUp 期间的暂停由 Manager 指令驱动。
    snapshotting: still('missing_edge'),
    catching_up: still('missing_edge'),
    running: cmd('stop'),
    paused: cmd('stop'),
    error: cmd('stop'),
    stopped: still('converged'),
    unknown: still('offline'),
  },
}

/** 某一侧下一步会发生什么。**按侧调用，永远不要传聚合值。** */
export function reconcileSide(
  desired: PipelineDesiredState,
  actual: PipelineActualState,
): SideReconcileOutcome {
  return MIRRORED_RECONCILE_TABLE[desired][actual]
}

/**
 * 聚合优先级表，镜像后端 `displayStateAggregationTier`。数值更小 = 更优先。
 *
 * 中间 6 个进行中状态（draft/prechecking/deploying/migrating/snapshotting/
 * catching_up）共享 tier 2，**彼此不分先后**。注意 `draft` 也在这一档——
 * 它在聚合里算「进行中」，不算终态，这一点反直觉，但实现就是这样
 * （后端 switch 的 default 分支）。
 */
const AGGREGATION_TIER: Record<PipelineActualState, number> = {
  error: 0,
  unknown: 1,
  draft: 2,
  prechecking: 2,
  deploying: 2,
  migrating: 2,
  snapshotting: 2,
  catching_up: 2,
  stopped: 3,
  paused: 4,
  running: 5,
}

export function deriveDisplayState(p: {
  sourceActualState: PipelineActualState
  targetActualState: PipelineActualState
  // **可选而不是 `string | null`**：`PipelineSummary` 里这三个 lastError 都是
  // `?: string`（缺席＝没有错误），照抄成 `| null` 会让这里与契约对不上。
  sourceLastError?: string
  targetLastError?: string
}): {
  displayState: PipelineActualState
  displayStateSide: PipelineSide
  displayLastError?: string
} {
  const sourceWins = AGGREGATION_TIER[p.sourceActualState] <= AGGREGATION_TIER[p.targetActualState]
  return sourceWins
    ? {
        displayState: p.sourceActualState,
        displayStateSide: 'source',
        displayLastError: p.sourceLastError,
      }
    : {
        displayState: p.targetActualState,
        displayStateSide: 'target',
        displayLastError: p.targetLastError,
      }
}

/**
 * 一条 pipeline 两侧各自的下一步。给测试与 dev 期排查用。
 *
 * **刻意返回两侧各一个结果，而不是一个合并结论**：合并就回到了
 * 「未收敛」那个把四类静止揉成一团的布尔值。
 */
export function reconcileBothSides(
  p: Pick<PipelineSummary, 'desiredState' | 'sourceActualState' | 'targetActualState'>,
): Record<PipelineSide, SideReconcileOutcome> {
  return {
    source: reconcileSide(p.desiredState, p.sourceActualState),
    target: reconcileSide(p.desiredState, p.targetActualState),
  }
}
