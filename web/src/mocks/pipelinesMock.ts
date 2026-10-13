import { validateResourceName } from './resourceNames'
// Pipeline 域的开发期 mock：让 `npm run dev` 在本地不接真实后端时也能走通
// pipeline 列表 / 详情 / 生命周期操作（启动/暂停/恢复/停止）的完整交互。
// 仅在 isApiMockEnabled() 为 true 时被调用（见 web/src/api/pipelines.ts）。
import { ApiError } from '@/api/client'
import type {
  GeneratedStrategyRulesDoc,
  GeneratedPipelineRulesUpdateInput,
} from '@/api/generated'
import type {
  PipelineBatchInput,
  PipelineBatchResponse,
  PipelineBatchResult,
  PipelineCloneInput,
} from '@/api/pipelineOperations'
import type {
  PipelineCreateInput,
  PipelineListQuery,
  PipelineRelease,
  PipelineReleaseListQuery,
  PipelineSummary,
  PipelineUpdateInput,
} from '@/api/pipelines'
import type { Paginated } from '@/api/types'
import {
  MOCK_DE_BJ_ORDERS,
  MOCK_DE_BJ_ORDERS_UNPROBED,
  MOCK_DE_SH_ORDERS,
  MOCK_NODE_BJ_01,
  MOCK_NODE_BJ_02,
  MOCK_NODE_SH_01,
  MOCK_PIPELINE_BJ_TO_SH_ORDERS,
  MOCK_PIPELINE_BJ_TO_SH_ORDERS_ERROR,
  MOCK_PIPELINE_SH_TO_BJ_ORDERS_UNKNOWN,
  MOCK_PIPELINE_SH_TO_BJ_USERS,
  MOCK_PIPELINE_SH_TO_BJ_USERS_LEGACY,
  MOCK_PIPELINE_SELF_LOOP_BJ,
  MOCK_PIPELINE_SH_TO_BJ_SPLIT_TERMINAL,
  MOCK_STRATEGY_ORDERS,
  MOCK_STRATEGY_SETTINGS_MIRROR,
  MOCK_TOPOLOGY_DEFAULT,
} from './seedIds'
import { deriveDisplayState } from './pipelineReconcileMirror'

/**
 * 本域 mock 写操作镜像的后端写入方（**就地声明，不设中心白名单**）。
 * 由 `web/src/mocks/backendWriteParity.test.ts` 逐条核对可达性，双向失败。
 *
 * 四个写操作在 internal/manager/http/pipelines.go 都有对应 repo 调用。
 */
export const backendWriteParity = {
  liveRepoWrites: [
    'CreatePipeline',
    'UpdatePipelineConfig',
    'SetPipelineDesiredState',
    'BumpPipelineCmdSeq',
  ],
  note: '见上方注释',
} as const

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

type MockPipeline = PipelineSummary & { rules?: GeneratedStrategyRulesDoc }

function seedPipelines(): MockPipeline[] {
  return [
    {
      id: MOCK_PIPELINE_BJ_TO_SH_ORDERS,
      name: 'bj-to-sh-orders',
      topologyId: MOCK_TOPOLOGY_DEFAULT,
      sourceEndpointId: MOCK_DE_BJ_ORDERS,
      sourceNodeId: MOCK_NODE_BJ_01,
      targetEndpointId: MOCK_DE_SH_ORDERS,
      targetNodeId: MOCK_NODE_SH_01,
      strategyId: MOCK_STRATEGY_ORDERS,

      strategyVersion: 1,
      initMode: 'full_and_incremental',
      desiredState: 'running',
      // 两侧都正常：聚合值取自 source（两侧同值时取哪一侧不影响结果）。
      sourceActualState: 'running',
      targetActualState: 'running',
      displayState: 'running',
      displayStateSide: 'source',
      sourceNodeLastHeartbeatAt: new Date(Date.now() - 1_000).toISOString(),
      targetNodeLastHeartbeatAt: new Date(Date.now() - 1_000).toISOString(),
      positionRevision: 0,
      recoveryExhausted: false,
      configVersion: 3,
      createdAt: '2026-08-02T01:00:00Z',
      updatedAt: '2026-09-20T06:00:00Z',
    },
    {
      id: MOCK_PIPELINE_SH_TO_BJ_USERS,
      name: 'sh-to-bj-users',
      topologyId: MOCK_TOPOLOGY_DEFAULT,
      sourceEndpointId: MOCK_DE_SH_ORDERS,
      sourceNodeId: MOCK_NODE_SH_01,
      targetEndpointId: MOCK_DE_BJ_ORDERS,
      targetNodeId: MOCK_NODE_BJ_01,
      strategyId: MOCK_STRATEGY_SETTINGS_MIRROR,
      strategyVersion: 2,
      initMode: 'incremental_only_gtid_set',
      // GTID 集合字符串示例，结构随 initMode 变化（见 web/src/api/pipelines.ts 文件头
      // 注释）；后端类型是不透明的 json.RawMessage，前端只读原样展示。
      startPosition: { gtidSet: '3E11FA47-71CA-11E1-9E33-C80AA9429562:1-58' },
      desiredState: 'running',
      sourceActualState: 'running',
      targetActualState: 'running',
      displayState: 'running',
      displayStateSide: 'source',
      sourceNodeLastHeartbeatAt: new Date(Date.now() - 1_000).toISOString(),
      targetNodeLastHeartbeatAt: new Date(Date.now() - 1_000).toISOString(),
      positionRevision: 0,
      recoveryExhausted: false,
      configVersion: 5,
      createdAt: '2026-08-10T02:00:00Z',
      updatedAt: '2026-09-18T03:30:00Z',
    },
    {
      id: MOCK_PIPELINE_SH_TO_BJ_ORDERS_UNKNOWN,
      name: 'sh-to-bj-orders-backfill',
      topologyId: MOCK_TOPOLOGY_DEFAULT,
      sourceEndpointId: MOCK_DE_SH_ORDERS,
      sourceNodeId: MOCK_NODE_SH_01,
      // 目标节点 bj-02 在 nodesMock 里是 offline：演示 actualState='unknown'
      // ——期望状态仍是 running，但 Manager 拿不到目标节点的确认，不是"没有
      // 意见"，是"存在但当前无法确认"（同 NodeActualLogLevel 的处理原则，见
      // web/src/api/pipelines.ts PipelineActualState 的注释）。
      targetEndpointId: MOCK_DE_BJ_ORDERS,
      targetNodeId: MOCK_NODE_BJ_02,
      strategyId: MOCK_STRATEGY_SETTINGS_MIRROR,
      strategyVersion: 3,
      initMode: 'incremental_only_binlog_pos',
      startPosition: { file: 'mysql-bin.000042', position: 154 },
      desiredState: 'running',
      // 源端正常、目标端离线。聚合优先级表里 Unknown(1) < Running(5)，所以
      // displayState 取自 target 那一侧。
      sourceActualState: 'running',
      targetActualState: 'unknown',
      // **离线侧的 lastError 是"离线前"的那条真实错误，不是离线的说明**——
      // 后端标离线时刻意不覆盖它（repo.MarkPipelineRoleUnknown）。这里特意
      // 放一条非空值，就是为了让界面必须证明自己把它标成"离线前最后一条
      // 错误"而不是"最近错误"：放空串的话那条渲染分支永远不会被走到。
      targetLastError:
        '目标库连接中断：dial tcp 10.1.1.12:3306: i/o timeout（节点失联前最后一次上报）',
      displayState: 'unknown',
      displayStateSide: 'target',
      sourceNodeLastHeartbeatAt: new Date(Date.now() - 1_000).toISOString(),
      // 与 nodesMock 里 bj-02 的 lastHeartbeatAt 对齐（同为 15 分钟前）：
      // 离线是按节点判的，两份 mock 说的必须是同一件事。
      targetNodeLastHeartbeatAt: new Date(Date.now() - 15 * 60_000).toISOString(),
      positionRevision: 0,
      recoveryExhausted: false,
      configVersion: 1,
      createdAt: '2026-09-15T08:00:00Z',
      updatedAt: '2026-09-23T02:00:00Z',
    },
    {
      id: MOCK_PIPELINE_BJ_TO_SH_ORDERS_ERROR,
      name: 'bj-to-sh-orders-narrow',
      topologyId: MOCK_TOPOLOGY_DEFAULT,
      sourceEndpointId: MOCK_DE_BJ_ORDERS,
      sourceNodeId: MOCK_NODE_BJ_01,
      // 和 pl-1001 是同一对源/目标（bj-orders→sh-orders），刻意保留——按更窄的
      // 库表范围拆出的并行 pipeline 是真实存在的用法（策略的 object_filters
      // 各自限定不同库表即可避免同步范围重叠，MGR-PL-14 的"范围重叠"指的是
      // 有重叠，不是指"同一对源/目标不能有第二条 pipeline"），不是数据错误。
      targetEndpointId: MOCK_DE_SH_ORDERS,
      targetNodeId: MOCK_NODE_SH_01,
      strategyId: MOCK_STRATEGY_SETTINGS_MIRROR,
      strategyVersion: 3,
      initMode: 'full_only',
      desiredState: 'running',
      // 源端正常在跑、目标端报错。Error 是全局最小 tier，所以聚合必为 error
      // ——这正是按侧拆分要保住的不变式 (a)：一侧的故障不可被另一侧的健康
      // 上报抹掉（修复前正是每秒被抹一次）。
      sourceActualState: 'running',
      targetActualState: 'error',
      targetLastError:
        "目标库权限校验失败：Access denied for user 'sync_user'@'%' to database 'orders_db'（GRANT 缺失，需管理员在目标库补授权）",
      displayState: 'error',
      displayStateSide: 'target',
      sourceNodeLastHeartbeatAt: new Date(Date.now() - 1_000).toISOString(),
      targetNodeLastHeartbeatAt: new Date(Date.now() - 1_000).toISOString(),
      // 已放弃自动恢复、在等人工。**这条必须有一个 true 的样本**：它和
      // "仍在自动重试"的 displayState 都是 error、配色一模一样，如果种子里
      // 没有一条 true，界面上那条"已停止自动恢复"的分支就从来没被走到过。
      positionRevision: 0,
      recoveryExhausted: true,
      configVersion: 2,
      createdAt: '2026-09-12T05:00:00Z',
      updatedAt: '2026-09-22T14:00:00Z',
    },
    {
      id: MOCK_PIPELINE_SH_TO_BJ_USERS_LEGACY,
      name: 'sh-to-bj-users-legacy',
      topologyId: MOCK_TOPOLOGY_DEFAULT,
      sourceEndpointId: MOCK_DE_SH_ORDERS,
      sourceNodeId: MOCK_NODE_SH_01,
      targetEndpointId: MOCK_DE_BJ_ORDERS,
      targetNodeId: MOCK_NODE_BJ_01,
      strategyId: MOCK_STRATEGY_SETTINGS_MIRROR,
      strategyVersion: 2,
      initMode: 'schema_only',
      // 已被 pl-1002 取代、手工停止的旧 pipeline：两侧实际状态都是 stopped
      // （不是"节点离线导致的 unknown"，是明确停下来了）。
      desiredState: 'stopped',
      sourceActualState: 'stopped',
      targetActualState: 'stopped',
      displayState: 'stopped',
      displayStateSide: 'source',
      sourceNodeLastHeartbeatAt: new Date(Date.now() - 1_000).toISOString(),
      targetNodeLastHeartbeatAt: new Date(Date.now() - 1_000).toISOString(),
      positionRevision: 0,
      recoveryExhausted: false,
      configVersion: 1,
      createdAt: '2026-08-05T03:00:00Z',
      updatedAt: '2026-09-01T00:00:00Z',
    },
    {
      id: MOCK_PIPELINE_SH_TO_BJ_SPLIT_TERMINAL,
      name: 'sh-to-bj-settings-halfstopped',
      topologyId: MOCK_TOPOLOGY_DEFAULT,
      sourceEndpointId: MOCK_DE_SH_ORDERS,
      sourceNodeId: MOCK_NODE_SH_01,
      targetEndpointId: MOCK_DE_BJ_ORDERS,
      targetNodeId: MOCK_NODE_BJ_01,
      strategyId: MOCK_STRATEGY_SETTINGS_MIRROR,
      strategyVersion: 2,
      initMode: 'incremental_only_latest',
      // 这就是 divergentSides 必须按侧各算一次的原因，也是为什么这条种子
      // 不能删：没有它，那条假阴性在本地 dev 和测试里都构造不出来，
      // "按侧比"这个改动就没有任何东西能证明它确实修了什么。
      desiredState: 'stopped',
      sourceActualState: 'stopped',
      targetActualState: 'running',
      displayState: 'stopped',
      displayStateSide: 'source',
      sourceNodeLastHeartbeatAt: new Date(Date.now() - 1_000).toISOString(),
      targetNodeLastHeartbeatAt: new Date(Date.now() - 1_000).toISOString(),
      positionRevision: 0,
      recoveryExhausted: false,
      configVersion: 4,
      createdAt: '2026-09-25T07:00:00Z',
      updatedAt: '2026-09-27T11:20:00Z',
    },
    {
      id: MOCK_PIPELINE_SELF_LOOP_BJ,
      name: 'bj-orders-selfloop',
      topologyId: MOCK_TOPOLOGY_DEFAULT,
      sourceEndpointId: MOCK_DE_BJ_ORDERS,
      sourceNodeId: MOCK_NODE_BJ_01,
      targetEndpointId: MOCK_DE_BJ_ORDERS_UNPROBED,
      // **自环：目标节点与源节点是同一个。** 这不是笔误，是后端受支持的
      // 真实拓扑（internal/manager/dispatch/ctl_config_pull.go 专门处理"一个节点同时是某条
      // pipeline 的源节点和目标节点"）。两个端点仍是两个不同的库，只是都
      // 跑在这台机器上——同中心约束因此天然满足（两者都属北京中心）。
      targetNodeId: MOCK_NODE_BJ_01,
      strategyId: MOCK_STRATEGY_SETTINGS_MIRROR,
      strategyVersion: 2,
      initMode: 'full_and_incremental',
      // 两侧状态**故意不同**（源端已 running、目标端还在 catching_up）。
      // 自环下两侧是同一个节点，最容易写成同一个值，而那样写这条种子就
      // 证明不了任何事：节点详情页那两行即使把两侧状态搞混、或者两行都
      // 渲染同一侧的值，看起来也完全正常。取值不同，错了就看得见。
      desiredState: 'running',
      sourceActualState: 'running',
      targetActualState: 'catching_up',
      // 聚合：CatchingUp(tier 2) < Running(tier 5)，取更小的一侧。
      displayState: 'catching_up',
      displayStateSide: 'target',
      // 自环下两侧是同一个节点，**这两个时间必须相同**——它们读的是同一行
      // nodes.last_heartbeat_at。写成两个不同的值会让"这台机器多久没消息了"
      // 在同一条线路的两行里给出两个答案。
      sourceNodeLastHeartbeatAt: new Date(Date.now() - 2_000).toISOString(),
      targetNodeLastHeartbeatAt: new Date(Date.now() - 2_000).toISOString(),
      positionRevision: 0,
      recoveryExhausted: false,
      configVersion: 2,
      createdAt: '2026-09-26T02:10:00Z',
      updatedAt: '2026-09-28T06:45:00Z',
    },
  ]
}

let pipelines: MockPipeline[] = seedPipelines()

/* 新建线路的 ID 序号（`pl-${1000 + pipelineSeq}`）。

它是种子 ID 契约的一个消费方，而且是最不像消费方的那一个：加种子时我
逐个找了依赖种子条数的断言，找到了拓扑/端点/注销三处，唯独没想到还有一个
「下一个可用 ID」的分配器——它不是断言，不会红，只会静默发出一个已被占用
的 ID。改成按现有种子的最大序号推导后，加种子不再需要记得同步这里。 */
let pipelineSeq =
  pipelines.reduce((max, item) => {
    const seq = Number.parseInt(item.id.replace(/^pl-/, ''), 10)
    return Number.isFinite(seq) ? Math.max(max, seq - 1000) : max
  }, 0) + 1

type MockPipelineRelease = PipelineRelease & { pipelineId: string }

function seedPipelineReleases(): MockPipelineRelease[] {
  return [
    {
      id: 'rel-2001',
      pipelineId: MOCK_PIPELINE_BJ_TO_SH_ORDERS,
      configVersion: 3,
      side: 'source',
      nodeId: MOCK_NODE_BJ_01,
      status: 'applied',
      createdAt: '2026-09-20T05:59:50Z',
      appliedAt: '2026-09-20T06:00:00Z',
    },
    {
      id: 'rel-2002',
      pipelineId: MOCK_PIPELINE_BJ_TO_SH_ORDERS,
      configVersion: 3,
      side: 'target',
      nodeId: MOCK_NODE_SH_01,
      status: 'applied',
      createdAt: '2026-09-20T05:59:50Z',
      appliedAt: '2026-09-20T06:00:05Z',
    },
    {
      id: 'rel-2003',
      pipelineId: MOCK_PIPELINE_BJ_TO_SH_ORDERS,
      configVersion: 2,
      side: 'source',
      nodeId: MOCK_NODE_BJ_01,
      status: 'applied',
      createdAt: '2026-09-10T00:59:56Z',
      appliedAt: '2026-09-10T01:00:04Z',
    },
    {
      id: 'rel-2004',
      pipelineId: MOCK_PIPELINE_SH_TO_BJ_ORDERS_UNKNOWN,
      configVersion: 1,
      side: 'source',
      nodeId: MOCK_NODE_SH_01,
      status: 'applied',
      createdAt: '2026-09-23T01:59:50Z',
      appliedAt: '2026-09-23T02:00:02Z',
    },
    {
      id: 'rel-2005',
      pipelineId: MOCK_PIPELINE_SH_TO_BJ_ORDERS_UNKNOWN,
      configVersion: 1,
      side: 'target',
      nodeId: MOCK_NODE_BJ_02,
      status: 'pending',
      createdAt: '2026-09-23T02:00:00Z',
    },
    {
      id: 'rel-2006',
      pipelineId: MOCK_PIPELINE_BJ_TO_SH_ORDERS_ERROR,
      configVersion: 2,
      side: 'target',
      nodeId: MOCK_NODE_SH_01,
      status: 'failed',
      message:
        "目标库权限校验失败：Access denied for user 'sync_user'@'%' to database 'orders_db'",
      createdAt: '2026-09-22T14:00:00Z',
      appliedAt: null,
    },
    {
      id: 'rel-2007',
      pipelineId: MOCK_PIPELINE_SH_TO_BJ_USERS,
      configVersion: 4,
      side: 'source',
      nodeId: MOCK_NODE_SH_01,
      status: 'rejected',
      message: '节点当前已应用版本 5 高于本次下发的版本 4，拒绝回退',
      createdAt: '2026-09-17T00:00:00Z',
      appliedAt: null,
    },
    {
      id: 'rel-2008',
      pipelineId: MOCK_PIPELINE_SH_TO_BJ_USERS,
      configVersion: 5,
      side: 'source',
      nodeId: MOCK_NODE_SH_01,
      status: 'applied',
      createdAt: '2026-09-18T02:59:50Z',
      appliedAt: '2026-09-18T03:00:00Z',
    },
    {
      id: 'rel-2009',
      pipelineId: MOCK_PIPELINE_SH_TO_BJ_USERS,
      configVersion: 5,
      side: 'target',
      nodeId: MOCK_NODE_BJ_01,
      status: 'applied',
      createdAt: '2026-09-18T02:59:50Z',
      appliedAt: '2026-09-18T03:00:03Z',
    },
  ]
}

let pipelineReleases: MockPipelineRelease[] = seedPipelineReleases()

export function listPipelinesByEndpoint(endpointId: string): PipelineSummary[] {
  return pipelines.filter(
    (p) => p.sourceEndpointId === endpointId || p.targetEndpointId === endpointId,
  )
}

export async function listPipelines(
  query: PipelineListQuery,
): Promise<Paginated<PipelineSummary>> {
  await delay(200)
  let filtered = pipelines.filter((p) => {
    if (query.topologyId && p.topologyId !== query.topologyId) {
      return false
    }
    if (query.keyword) {
      const keyword = query.keyword.trim().toLowerCase()
      if (keyword.length > 0 && !p.name.toLowerCase().includes(keyword)) {
        return false
      }
    }
    return true
  })
  const total = filtered.length
  const start = (query.page - 1) * query.pageSize
  const items = filtered.slice(start, start + query.pageSize)
  return { items, total, page: query.page, pageSize: query.pageSize }
}

export async function getPipeline(id: string): Promise<PipelineSummary> {
  await delay(150)
  const pipeline = pipelines.find((item) => item.id === id)
  if (!pipeline) {
    throw new ApiError('pipeline 不存在', 404, 'not_found')
  }
  return pipeline
}

export async function listPipelineReleases(
  id: string,
  query: PipelineReleaseListQuery,
): Promise<Paginated<PipelineRelease>> {
  await delay(150)
  if (!pipelines.some((item) => item.id === id)) {
    throw new ApiError('pipeline 不存在', 404, 'not_found')
  }
  const filtered = pipelineReleases
    .filter((release) => release.pipelineId === id)
    .sort((a, b) => b.configVersion - a.configVersion)
  const total = filtered.length
  const start = (query.page - 1) * query.pageSize
  const items: PipelineRelease[] = filtered
    .slice(start, start + query.pageSize)
    .map((release) => {
      const { pipelineId: _pipelineId, ...rest } = release
      return rest
    })
  return { items, total, page: query.page, pageSize: query.pageSize }
}

/**
 * 创建向导（PipelineCreatePage）调用这个函数落地第一步请求；desiredState/
 * actualState 硬编码为 stopped/draft，与真实后端 internal/manager/http/pipelines.go create handler
 * 的行为一致（创建不等于启动，启动是向导第二个独立请求，见
 * useCreatePipelineMutation/useStartPipelineMutation 的调用方注释）。
 */
export async function createPipeline(input: PipelineCreateInput): Promise<PipelineSummary> {
  await delay(250)
  return appendPipeline(input)
}

export async function clonePipeline(
  id: string,
  input: PipelineCloneInput,
): Promise<PipelineSummary> {
  const { fetchMe } = await import('./authMock')
  if ((await fetchMe()).user?.role !== 'admin')
    throw new ApiError('没有权限执行此操作', 403, 'forbidden')
  const source = await getPipeline(id)
  if (!input.name.trim()) throw new ApiError('请填写克隆线路的新名称', 400, 'invalid_request')
  if (pipelines.some((item) => item.name.toLowerCase() === input.name.trim().toLowerCase()))
    throw new ApiError('pipeline 名称已存在', 409, 'pipeline_name_conflict')
  const value: PipelineCreateInput = {
    name: validateResourceName(input.name, pipelines, 'pipeline_name_conflict'),
    topologyId: input.topologyId ?? source.topologyId,
    sourceEndpointId: input.sourceEndpointId ?? source.sourceEndpointId,
    sourceNodeId: input.sourceNodeId ?? source.sourceNodeId,
    targetEndpointId: input.targetEndpointId ?? source.targetEndpointId,
    targetNodeId: input.targetNodeId ?? source.targetNodeId,
    strategyId: input.strategyId ?? source.strategyId,
    strategyVersion: input.strategyVersion ?? source.strategyVersion,
    initMode: input.initMode ?? source.initMode,
    startPosition:
      input.startPosition ??
      (input.initMode && input.initMode !== source.initMode
        ? undefined
        : structuredClone(source.startPosition)),
  }
  if (
    value.sourceEndpointId === source.sourceEndpointId &&
    value.targetEndpointId === source.targetEndpointId &&
    value.strategyId === source.strategyId &&
    value.strategyVersion === source.strategyVersion
  ) {
    throw new ApiError(
      '同步范围与原线路重叠，请选择其他目标端点或策略',
      409,
      'pipeline_scope_overlap',
    )
  }
  const [{ getEndpoint }, { getNode }] = await Promise.all([
    import('./endpointsMock'),
    import('./nodesMock'),
  ])
  const [endpoint, node] = await Promise.all([
    getEndpoint(value.targetEndpointId),
    getNode(value.targetNodeId),
  ])
  if (endpoint.centerId !== node.centerId)
    throw new ApiError('节点与端点不在同一中心', 400, 'center_mismatch')
  return appendPipeline(value)
}

export async function batchPipelines(
  input: PipelineBatchInput,
): Promise<PipelineBatchResponse> {
  const { fetchMe } = await import('./authMock')
  if ((await fetchMe()).user?.role !== 'admin')
    throw new ApiError('没有权限执行此操作', 403, 'forbidden')
  if (
    input.ids.length === 0 ||
    input.ids.length > 100 ||
    new Set(input.ids).size !== input.ids.length
  )
    throw new ApiError('请选择1至100条不同线路', 400, 'invalid_ids')
  const operations = {
    start: startPipeline,
    pause: pausePipeline,
    resume: resumePipeline,
    stop: stopPipeline,
  }
  if (!Object.hasOwn(operations, input.action))
    throw new ApiError('批量操作仅支持启动、暂停、恢复和停止', 400, 'invalid_action')
  const results: PipelineBatchResult[] = []
  for (const id of input.ids) {
    try {
      const current = pipelines.find((item) => item.id === id)
      if (
        current?.desiredState === 'stopped' &&
        current.sourceActualState === 'draft' &&
        current.targetActualState === 'draft'
      ) {
        if (input.action === 'pause')
          throw new ApiError(
            '当前状态不允许此操作，未启动线路请先完成预检查并启动',
            409,
            'invalid_pipeline_transition',
          )
        if (input.action === 'start' || input.action === 'resume')
          throw new ApiError('请先完成当前配置的预检查', 409, 'precheck_required')
      }
      await operations[input.action](id)
      results.push({ id, success: true, code: 'ok', message: '操作已接受' })
    } catch (error) {
      results.push({
        id,
        success: false,
        code:
          error instanceof ApiError
            ? (error.code ?? 'pipeline_intent_failed')
            : 'pipeline_intent_failed',
        message: error instanceof Error ? error.message : '服务器内部错误',
      })
    }
  }
  return { results }
}

function appendPipeline(input: PipelineCreateInput): PipelineSummary {
  const now = new Date().toISOString()
  const record: MockPipeline = {
    id: `pl-${String(1000 + pipelineSeq).padStart(4, '0')}`,
    name: validateResourceName(input.name, pipelines, 'pipeline_name_conflict'),
    topologyId: input.topologyId ?? '00000000-0000-4000-8000-000000000001',
    sourceEndpointId: input.sourceEndpointId,
    sourceNodeId: input.sourceNodeId,
    targetEndpointId: input.targetEndpointId,
    targetNodeId: input.targetNodeId,
    strategyId: input.strategyId ?? `pipeline-${pipelineSeq}`,
    strategyVersion: input.strategyVersion ?? 1,
    rules: input.rules ? structuredClone(input.rules) : undefined,
    initMode: input.initMode,
    startPosition: input.startPosition,
    desiredState: 'stopped',

    sourceActualState: 'draft',
    targetActualState: 'draft',
    displayState: 'draft',
    displayStateSide: 'source',
    // 两侧节点的心跳与这条新 pipeline 无关，这里不编造——真实后端取的是
    // nodes.last_heartbeat_at，新建 pipeline 不会改变它。mock 给 null 表示
    // "本 mock 不掌握这个值"，比随手填一个 now 更诚实（填 now 会让界面在
    // 目标节点其实早就离线时显示"刚刚还有心跳"）。
    sourceNodeLastHeartbeatAt: null,
    targetNodeLastHeartbeatAt: null,
    recoveryExhausted: false,
    positionRevision: 0,
    configVersion: 0,
    createdAt: now,
    updatedAt: now,
  }
  pipelineSeq += 1
  pipelines = [...pipelines, record]
  return record
}

export function preparePipelinePair(
  forward: PipelineCreateInput,
  reverse: PipelineCreateInput,
) {
  const forwardName = validateResourceName(forward.name, pipelines, 'pipeline_name_conflict')
  validateResourceName(
    reverse.name,
    [...pipelines, { id: 'pending-forward', name: forwardName }],
    'pipeline_name_conflict',
  )
  return {
    commit: () => ({ forward: appendPipeline(forward), reverse: appendPipeline(reverse) }),
  }
}

export async function updatePipeline(
  id: string,
  input: PipelineUpdateInput,
): Promise<PipelineSummary> {
  await delay(200)
  const index = pipelines.findIndex((item) => item.id === id)
  const existing = index >= 0 ? pipelines[index] : undefined
  if (index < 0 || !existing) {
    throw new ApiError('pipeline 不存在', 404, 'not_found')
  }
  const updated: MockPipeline = {
    ...existing,
    name:
      input.name === undefined
        ? existing.name
        : validateResourceName(input.name, pipelines, 'pipeline_name_conflict', id),
    strategyId: input.strategyId ?? existing.strategyId,
    strategyVersion: input.strategyVersion ?? existing.strategyVersion,
    initMode: input.initMode ?? existing.initMode,
    updatedAt: new Date().toISOString(),
  }
  pipelines[index] = updated
  return updated
}

/** 软删除（MGR-PL-13 两步删除的第一步）。mock 简化成直接从列表移除，不模拟
 * 软删除中间态——回收流程是纯后端异步行为，前端本来就看不到那个中间态。 */
export async function deletePipeline(id: string): Promise<void> {
  await delay(200)
  const existing = pipelines.find((item) => item.id === id)
  if (!existing) {
    throw new ApiError('pipeline 不存在', 404, 'not_found')
  }
  if (
    existing.desiredState !== 'stopped' ||
    !['draft', 'stopped'].includes(existing.sourceActualState) ||
    !['draft', 'stopped'].includes(existing.targetActualState)
  ) {
    throw new ApiError(
      '请先停止线路并等待两端停止后再删除',
      409,
      'pipeline_delete_requires_stopped',
    )
  }
  pipelines = pipelines.filter((item) => item.id !== id)
}

function transition(id: string, desiredState: MockPipeline['desiredState']) {
  const index = pipelines.findIndex((item) => item.id === id)
  const existing = index >= 0 ? pipelines[index] : undefined
  if (index < 0 || !existing) {
    throw new ApiError('pipeline 不存在', 404, 'not_found')
  }
  const updated: MockPipeline = {
    ...existing,
    desiredState,
    // 两侧实际状态与各自的 lastError 原样保留（见上面"为什么不一起收敛"）。
    // 展示值由两侧原值重算，而不是沿用旧的——它是两侧的函数，**手写它就会
    // 和两侧悄悄对不上**。这里两侧没变，所以结果也不会变；重算是为了让
    // "展示值永远等于 tier 表对当前两侧的结果"成为一条无例外的不变式。
    ...deriveDisplayState(existing),
    updatedAt: new Date().toISOString(),
  }
  pipelines[index] = updated
  return updated
}

export async function startPipeline(id: string): Promise<PipelineSummary> {
  await delay(200)
  return transition(id, 'running')
}

export async function pausePipeline(id: string): Promise<PipelineSummary> {
  await delay(200)
  return transition(id, 'paused')
}

export async function resumePipeline(id: string): Promise<PipelineSummary> {
  await delay(200)
  return transition(id, 'running')
}

export async function stopPipeline(id: string): Promise<PipelineSummary> {
  await delay(200)
  return transition(id, 'stopped')
}

/**
 * 与真实后端 `POST /pipelines/:id/retry` 对齐：重置"已放弃自动恢复"这个
 * 标记，**不改 desiredState**，也不假装错误已经消失。
 *
 * 刻意不把 targetActualState 从 error 改回 running：重新允许自动恢复不等于
 * 恢复已经成功。mock 如果顺手把状态也改好，本地看到的就是一个"点一下就
 * 治好了"的假象，而真实链路只是重新开始尝试。
 */
export async function retryPipeline(id: string): Promise<PipelineSummary> {
  await delay(200)
  const index = pipelines.findIndex((item) => item.id === id)
  const existing = index >= 0 ? pipelines[index] : undefined
  if (index < 0 || !existing) {
    throw new ApiError('pipeline 不存在', 404, 'not_found')
  }
  const updated: MockPipeline = {
    ...existing,
    recoveryExhausted: false,
    updatedAt: new Date().toISOString(),
  }
  pipelines[index] = updated
  return updated
}

export function readInlinePipelineRules(id: string): GeneratedStrategyRulesDoc | undefined {
  const record = pipelines.find((item) => item.id === id)
  return record?.rules ? structuredClone(record.rules) : undefined
}

export function writeInlinePipelineRules(
  id: string,
  input: GeneratedPipelineRulesUpdateInput,
  newVersion = true,
): void {
  const record = pipelines.find((item) => item.id === id)
  if (!record) throw new ApiError('pipeline 不存在', 404, 'not_found')
  if (record.configVersion !== input.configVersion)
    throw new ApiError('配置已变化，请刷新后重试', 409, 'pipeline_config_version_conflict')
  pipelines = pipelines.map((item) =>
    item.id === id
      ? {
          ...item,
          rules: structuredClone(input.rules),
          strategyId: 'pipeline-' + id,
          strategyVersion: item.strategyVersion + (newVersion ? 1 : 0),
          configVersion: item.configVersion + 1,
          updatedAt: new Date().toISOString(),
        }
      : item,
  )
}
