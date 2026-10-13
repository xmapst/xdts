import { validateResourceName } from './resourceNames'
import { ApiError } from '@/api/client'
import type {
  NodeDeregisterBlocker,
  NodeDeregisterResult,
  NodeDetail,
  NodeListQuery,
  NodeLogLevelInput,
  NodeLogLevelResult,
  NodeStatus,
  NodeSummary,
  NodeUpdateInput,
  RegisterCode,
  RegisterCodeCreateInput,
  RegisterCodeCreated,
} from '@/api/nodes'
import type { Paginated } from '@/api/types'
import {
  MOCK_CENTER_BJ,
  MOCK_CENTER_MGR,
  MOCK_CENTER_SH,
  MOCK_NODE_BJ_01,
  MOCK_NODE_BJ_02,
  MOCK_NODE_MGR_01,
  MOCK_NODE_SH_01,
  MOCK_NODE_SH_02,
  MOCK_NODE_SH_03,
  MOCK_NODE_SH_04,
  MOCK_PIPELINE_BJ_TO_SH_ORDERS,
  MOCK_PIPELINE_BJ_TO_SH_ORDERS_ERROR,
  MOCK_PIPELINE_SH_TO_BJ_USERS,
  MOCK_PIPELINE_SH_TO_BJ_USERS_LEGACY,
  MOCK_PIPELINE_SH_TO_BJ_SPLIT_TERMINAL,
  MOCK_PIPELINE_SELF_LOOP_BJ,
  MOCK_PIPELINE_SH_TO_BJ_ORDERS_UNKNOWN,
} from './seedIds'

/* 本域 mock 写操作镜像的后端写入方（就地声明，不设中心白名单）。
由 `web/src/mocks/backendWriteParity.test.ts` 逐条核对可达性，双向失败。 */
export const backendWriteParity = {
  liveRepoWrites: [
    'UpdateNode',
    'RevokeNodeToken',
    'SetNodeLogLevel',
    'GenerateNodeRegisterCode',
    'RevokeNodeRegisterCode',
    'SoftDeleteNode',
  ],
  note: '见上方注释',
} as const

type MockNode = NodeDetail

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function seedNodes(): MockNode[] {
  return [
    {
      id: MOCK_NODE_MGR_01,
      name: 'node-mgr-01',
      centerId: MOCK_CENTER_MGR,
      status: 'online',
      version: 'v0.9.2',
      hostname: 'mgr-node-01.internal',
      maxPipelines: 0,
      labels: ['role:inspector'],
      lastHeartbeatAt: new Date(Date.now() - 5_000).toISOString(),
      registeredAt: '2026-08-01T02:00:00Z',
      ips: ['10.0.1.11'],
      osArch: 'linux/amd64',
      desiredLogLevel: 'info',
      desiredLogShipLevel: 'warn',
      actualLogLevel: 'unknown',
      actualLogShipLevel: 'unknown',
      runtime: {
        cpuPercent: 12.4,
        memPercent: 38.1,
        // 分子/分母与上面的百分比**真的对得上**（3272765080 / 8589934592 = 38.1%）：
        // 这组字段存在的唯一理由是让运维能在界面上验算，mock 里随手填两个
        // 对不上的数，等于在本地演示一个"数据自相矛盾"的假故障。
        memRssBytes: 3272765080,
        memTotalBytes: 8589934592,
        stateDbSizeBytes: 214_748_364,
        pipelineCount: 0,
        uptimeSeconds: 864_000,
      },
      pipelines: [],
    },
    {
      id: MOCK_NODE_BJ_01,
      name: 'node-bj-01',
      centerId: MOCK_CENTER_BJ,
      status: 'online',
      version: 'v0.9.2',
      hostname: 'bj-node-01.internal',
      maxPipelines: 0,
      labels: ['az:bj-a'],
      lastHeartbeatAt: new Date(Date.now() - 2_000).toISOString(),
      registeredAt: '2026-08-03T09:12:00Z',
      ips: ['10.1.1.11'],
      osArch: 'linux/amd64',
      desiredLogLevel: 'info',
      desiredLogShipLevel: 'info',
      actualLogLevel: 'unknown',
      actualLogShipLevel: 'unknown',
      runtime: {
        cpuPercent: 45.7,
        memPercent: 61.2,
        // 分子/分母与上面的百分比**真的对得上**（10514079941 / 17179869184 = 61.2%）：
        // 这组字段存在的唯一理由是让运维能在界面上验算，mock 里随手填两个
        // 对不上的数，等于在本地演示一个"数据自相矛盾"的假故障。
        memRssBytes: 10514079941,
        memTotalBytes: 17179869184,
        stateDbSizeBytes: 12_884_901_888,
        // 承载 6 条**线路**；注意下方 pipelines 数组是 7 **行**——自环的
        // pl-1007 占两行（source/target 各一行）。两个数字本来就不相等，
        // 这里数的是线路数，不是角色数。
        pipelineCount: 6,
        uptimeSeconds: 432_000,
      },
      // 与 web/src/mocks/pipelinesMock.ts 的种子数据保持一致（见 web/src/mocks/seedIds.ts 头部注释）：
      // bj-01 同时是 pl-1001/pl-1004 的源节点、pl-1002/pl-1005 的目标节点。
      pipelines: [
        {
          pipelineId: MOCK_PIPELINE_BJ_TO_SH_ORDERS,
          pipelineName: 'bj-to-sh-orders',
          role: 'source',
          sideActualState: 'running',
        },
        {
          pipelineId: MOCK_PIPELINE_SH_TO_BJ_USERS,
          pipelineName: 'sh-to-bj-users',
          role: 'target',
          sideActualState: 'running',
        },
        {
          pipelineId: MOCK_PIPELINE_BJ_TO_SH_ORDERS_ERROR,
          pipelineName: 'bj-to-sh-orders-narrow',
          role: 'source',
          sideActualState: 'error',
        },
        {
          pipelineId: MOCK_PIPELINE_SH_TO_BJ_USERS_LEGACY,
          pipelineName: 'sh-to-bj-users-legacy',
          role: 'target',
          sideActualState: 'stopped',
        },
        {
          pipelineId: MOCK_PIPELINE_SH_TO_BJ_SPLIT_TERMINAL,
          pipelineName: 'sh-to-bj-settings-halfstopped',
          role: 'target',
          sideActualState: 'running',
        },
        // ↓↓ 自环（pl-1007）：**同一条 pipeline 在这里占两行**，source 与
        // target 各一行。后端 internal/manager/http/nodes.go 的 toNodeDetailView 就是按侧
        // 逐个 append 产出的（自环时两个 if 都命中），这里逐字对齐那个形状。
        // 两行的 sideActualState 取值不同，见 web/src/mocks/pipelinesMock.ts 该种子的注释。
        {
          pipelineId: MOCK_PIPELINE_SELF_LOOP_BJ,
          pipelineName: 'bj-orders-selfloop',
          role: 'source',
          sideActualState: 'running',
        },
        {
          pipelineId: MOCK_PIPELINE_SELF_LOOP_BJ,
          pipelineName: 'bj-orders-selfloop',
          role: 'target',
          sideActualState: 'catching_up',
        },
      ],
    },
    {
      id: MOCK_NODE_BJ_02,
      name: 'node-bj-02',
      centerId: MOCK_CENTER_BJ,
      status: 'offline',
      version: 'v0.9.1',
      hostname: 'bj-node-02.internal',
      maxPipelines: 0,
      labels: ['az:bj-b'],
      lastHeartbeatAt: new Date(Date.now() - 15 * 60_000).toISOString(),
      registeredAt: '2026-08-05T09:12:00Z',
      ips: ['10.1.1.12'],
      osArch: 'linux/arm64',
      desiredLogLevel: 'info',
      desiredLogShipLevel: 'info',
      actualLogLevel: 'unknown',
      actualLogShipLevel: 'unknown',
      runtime: {
        cpuPercent: null,
        memPercent: null,
        // memPercent 为 null 时分子分母同样是 null——三个数同源于一次采样，
        // 不存在"百分比不知道但分子知道"这种组合。
        memRssBytes: null,
        memTotalBytes: null,
        stateDbSizeBytes: 3_221_225_472,
        pipelineCount: 1,
        uptimeSeconds: null,
      },
      // bj-02 离线但仍是 pl-1003 的目标节点——演示 pipeline.actualState=
      // 'unknown'「目标节点离线、Manager 无法确认实际状态」这个场景
      // （见 web/src/api/pipelines.ts 对 PipelineActualState.unknown 的注释）。
      pipelines: [
        {
          pipelineId: MOCK_PIPELINE_SH_TO_BJ_ORDERS_UNKNOWN,
          pipelineName: 'sh-to-bj-orders-backfill',
          role: 'target',
          sideActualState: 'unknown',
        },
      ],
    },
    {
      id: MOCK_NODE_SH_01,
      name: 'node-sh-01',
      centerId: MOCK_CENTER_SH,
      status: 'online',
      version: 'v0.9.2',
      hostname: 'sh-node-01.internal',
      maxPipelines: 0,
      labels: [],
      lastHeartbeatAt: new Date(Date.now() - 1_000).toISOString(),
      registeredAt: '2026-08-06T01:00:00Z',
      ips: ['10.2.1.11'],
      osArch: 'linux/amd64',
      desiredLogLevel: 'debug',
      desiredLogShipLevel: 'info',
      actualLogLevel: 'unknown',
      actualLogShipLevel: 'unknown',
      runtime: {
        cpuPercent: 22.9,
        memPercent: 40.5,
        // 分子/分母与上面的百分比**真的对得上**（6957847020 / 17179869184 = 40.5%）：
        // 这组字段存在的唯一理由是让运维能在界面上验算，mock 里随手填两个
        // 对不上的数，等于在本地演示一个"数据自相矛盾"的假故障。
        memRssBytes: 6957847020,
        memTotalBytes: 17179869184,
        stateDbSizeBytes: 8_589_934_592,
        pipelineCount: 6,
        uptimeSeconds: 172_800,
      },
      pipelines: [
        {
          pipelineId: MOCK_PIPELINE_BJ_TO_SH_ORDERS,
          pipelineName: 'bj-to-sh-orders',
          role: 'target',
          sideActualState: 'running',
        },
        {
          pipelineId: MOCK_PIPELINE_SH_TO_BJ_USERS,
          pipelineName: 'sh-to-bj-users',
          role: 'source',
          sideActualState: 'running',
        },
        {
          pipelineId: MOCK_PIPELINE_SH_TO_BJ_ORDERS_UNKNOWN,
          pipelineName: 'sh-to-bj-orders-backfill',
          role: 'source',
          sideActualState: 'unknown',
        },
        {
          pipelineId: MOCK_PIPELINE_BJ_TO_SH_ORDERS_ERROR,
          pipelineName: 'bj-to-sh-orders-narrow',
          role: 'target',
          sideActualState: 'error',
        },
        {
          pipelineId: MOCK_PIPELINE_SH_TO_BJ_USERS_LEGACY,
          pipelineName: 'sh-to-bj-users-legacy',
          role: 'source',
          sideActualState: 'stopped',
        },
        {
          pipelineId: MOCK_PIPELINE_SH_TO_BJ_SPLIT_TERMINAL,
          pipelineName: 'sh-to-bj-settings-halfstopped',
          role: 'source',
          sideActualState: 'stopped',
        },
      ],
    },
    {
      id: MOCK_NODE_SH_02,
      name: 'node-sh-02',
      centerId: MOCK_CENTER_SH,
      status: 'disabled',
      version: 'v0.9.0',
      hostname: 'sh-node-02.internal',
      maxPipelines: 0,
      labels: ['az:sh-b'],
      lastHeartbeatAt: '2026-09-10T08:00:00Z',
      registeredAt: '2026-07-20T05:30:00Z',
      ips: ['10.2.1.12'],
      osArch: 'linux/amd64',
      desiredLogLevel: 'warn',
      desiredLogShipLevel: 'warn',
      actualLogLevel: 'unknown',
      actualLogShipLevel: 'unknown',
      runtime: {
        cpuPercent: null,
        memPercent: null,
        // memPercent 为 null 时分子分母同样是 null——三个数同源于一次采样，
        // 不存在"百分比不知道但分子知道"这种组合。
        memRssBytes: null,
        memTotalBytes: null,
        stateDbSizeBytes: 1_073_741_824,
        pipelineCount: 0,
        uptimeSeconds: null,
      },
      pipelines: [],
    },
    {
      id: MOCK_NODE_SH_03,
      name: 'node-sh-03',
      centerId: MOCK_CENTER_SH,
      // revoked：token 已吊销，不可逆，界面上不得和 disabled 合并展示。
      status: 'revoked',
      version: 'v0.8.7',
      hostname: 'sh-node-03.internal',
      maxPipelines: 0,
      labels: [],
      lastHeartbeatAt: '2026-09-01T03:00:00Z',
      registeredAt: '2026-06-15T00:00:00Z',
      ips: ['10.2.1.13'],
      osArch: 'linux/amd64',
      desiredLogLevel: 'info',
      desiredLogShipLevel: 'info',
      actualLogLevel: 'unknown',
      actualLogShipLevel: 'unknown',
      runtime: {
        cpuPercent: null,
        memPercent: null,
        // memPercent 为 null 时分子分母同样是 null——三个数同源于一次采样，
        // 不存在"百分比不知道但分子知道"这种组合。
        memRssBytes: null,
        memTotalBytes: null,
        stateDbSizeBytes: 0,
        pipelineCount: 0,
        uptimeSeconds: null,
      },
      pipelines: [],
    },
    {
      id: MOCK_NODE_SH_04,
      name: 'node-sh-04',
      centerId: MOCK_CENTER_SH,

      status: 'registering',
      version: 'v0.9.2',
      hostname: 'sh-node-04.internal',
      maxPipelines: 0,
      labels: [],
      lastHeartbeatAt: null,
      registeredAt: new Date(Date.now() - 20_000).toISOString(),
      ips: [],
      osArch: 'linux/amd64',
      desiredLogLevel: 'info',
      desiredLogShipLevel: 'info',
      actualLogLevel: 'unknown',
      actualLogShipLevel: 'unknown',
      runtime: {
        cpuPercent: null,
        memPercent: null,
        // memPercent 为 null 时分子分母同样是 null——三个数同源于一次采样，
        // 不存在"百分比不知道但分子知道"这种组合。
        memRssBytes: null,
        memTotalBytes: null,
        stateDbSizeBytes: null,
        // 三态在种子里都要有，否则有一态在本地和测试里构造不出来：
        //   null → 本节点（从未心跳）
        //   0    → node-mgr-01（在线、确实承载 0 条）
        //   n    → bj-01 / sh-01 等
        pipelineCount: null,
        uptimeSeconds: null,
      },
      pipelines: [],
    },
  ]
}

let nodes: MockNode[] = seedNodes()
let registerCodes: (RegisterCode & { codeHash: string })[] = [
  {
    id: 'rc-0001',
    centerId: MOCK_CENTER_BJ,
    presetName: 'node-bj-03',
    presetLabels: ['az:bj-a'],
    expiresAt: new Date(Date.now() + 7 * 86_400_000).toISOString(),
    usedCount: 0,
    createdBy: 'admin',
    createdAt: new Date(Date.now() - 3600_000).toISOString(),
    revokedAt: null,
    codeHash: 'demo-hash-0001',
  },
]
let registerCodeSeq = 2

/** 供 centersMock 校验「删除中心前必须无归属节点」（MGR-CTR-04）。 */
export function countNodesByCenter(centerId: string): number {
  return nodes.filter((node) => node.centerId === centerId).length
}

/**
 * 供 endpointsMock 派发探测任务前校验「节点必须与数据端点同中心且在线」
 * （MGR-DS-02）。只返回校验需要的最小字段，不把整个节点对象暴露给数据端点域。
 */
export function getNodeForProbe(
  nodeId: string,
): { centerId: string; status: NodeStatus } | undefined {
  const node = nodes.find((item) => item.id === nodeId)
  if (!node) {
    return undefined
  }
  return { centerId: node.centerId, status: node.status }
}

function toSummary(node: MockNode): NodeSummary {
  const {
    id,
    name,
    centerId,
    status,
    version,
    hostname,
    labels,
    maxPipelines,
    lastHeartbeatAt,
    registeredAt,
  } = node
  return {
    id,
    name,
    centerId,
    status,
    version,
    hostname,
    labels,
    maxPipelines,
    lastHeartbeatAt,
    registeredAt,
    hasPipelines: node.pipelines.length > 0,
  }
}

export async function listNodes(query: NodeListQuery): Promise<Paginated<NodeSummary>> {
  await delay(200)
  let filtered = nodes.filter((node) => {
    if (query.centerId && node.centerId !== query.centerId) {
      return false
    }
    if (query.status && node.status !== query.status) {
      return false
    }
    if (query.keyword) {
      const keyword = query.keyword.trim().toLowerCase()
      if (
        keyword.length > 0 &&
        !node.name.toLowerCase().includes(keyword) &&
        !node.hostname.toLowerCase().includes(keyword)
      ) {
        return false
      }
    }
    return true
  })
  const total = filtered.length
  const start = (query.page - 1) * query.pageSize
  const items = filtered.slice(start, start + query.pageSize).map(toSummary)
  return { items, total, page: query.page, pageSize: query.pageSize }
}

export async function getNode(id: string): Promise<NodeDetail> {
  await delay(150)
  const node = nodes.find((item) => item.id === id)
  if (!node) {
    throw new ApiError('节点不存在', 404, 'not_found')
  }
  return { ...node, hasPipelines: node.pipelines.length > 0 }
}

function assertUnusedNode(existing: MockNode) {
  if (existing.pipelines.length > 0) {
    // **按 pipelineId 去重**：自环时 `pipelines` 里同一条线路占两行
    // （role=source / role=target），而真实后端的阻断清单是逐条 **pipeline**
    // 产出的（repo 层查的是 pipelines 表，一条线路一行），不是逐个角色。
    // 不去重会让弹窗里同一条线路出现两次，且 NodeDeregisterBlockedNotice 的
    // `rowKey="pipelineId"` 会撞 key——那是一个 mock 自己造出来、真实后端
    // 不存在的故障，属于"mock 表达了契约没有的东西"。
    const seen = new Set<string>()
    const blockers: NodeDeregisterBlocker[] = []
    for (const ref of existing.pipelines) {
      if (seen.has(ref.pipelineId)) {
        continue
      }
      seen.add(ref.pipelineId)
      blockers.push({
        pipelineId: ref.pipelineId,
        pipelineName: ref.pipelineName,
        cleanupState: 'done',
        deletedAt: null,
      })
    }
    throw new ApiError(
      `节点仍被 ${blockers.length} 条 pipeline 引用，不能禁用、吊销 token 或注销（含待清理未完成的）`,
      409,
      'node_has_dependents',
      undefined,
      blockers,
    )
  }
}

export async function updateNode(id: string, input: NodeUpdateInput): Promise<NodeSummary> {
  await delay(200)
  const index = nodes.findIndex((item) => item.id === id)
  const existing = index >= 0 ? nodes[index] : undefined
  if (index < 0 || !existing) {
    throw new ApiError('节点不存在', 404, 'not_found')
  }

  if (input.disabled === true) assertUnusedNode(existing)
  let status = existing.status
  if (input.disabled === true) {
    status = 'disabled'
  } else if (input.disabled === false) {
    status = 'offline'
  }
  const updated: MockNode = {
    ...existing,
    name:
      input.name === undefined
        ? existing.name
        : validateResourceName(input.name, nodes, 'node_name_conflict', id),
    labels: input.labels ?? existing.labels,
    maxPipelines: input.maxPipelines ?? existing.maxPipelines,
    status,
  }
  nodes[index] = updated
  return toSummary(updated)
}

export async function revokeNodeToken(id: string): Promise<void> {
  await delay(200)
  const index = nodes.findIndex((item) => item.id === id)
  const existing = index >= 0 ? nodes[index] : undefined
  if (index < 0 || !existing) {
    throw new ApiError('节点不存在', 404, 'not_found')
  }
  assertUnusedNode(existing)
  // 吊销后节点进入不可逆的 revoked 状态，需要新注册码重新注册才能恢复
  // （不再像早前版本那样借用 offline 近似表达——revoked 现在是真实枚举值）。
  nodes[index] = { ...existing, status: 'revoked' }
}

export async function deregisterNode(id: string): Promise<NodeDeregisterResult> {
  await delay(200)
  const index = nodes.findIndex((item) => item.id === id)
  const existing = index >= 0 ? nodes[index] : undefined
  if (index < 0 || !existing) {
    throw new ApiError('节点不存在', 404, 'not_found')
  }
  assertUnusedNode(existing)
  nodes.splice(index, 1)
  return { id, cleanupState: 'pending' }
}

/**
 * 与真实后端 `POST /nodes/{id}/log-level` 行为对齐：只持久化 desired* 两列，
 * actual* 恒为 `'unknown'`——`cmd.node` 已定义但零生产者（见 `web/src/api/nodes.ts`
 * 的取证），确认通道不会填充它，mock 也不应该假装
 * 提交后就"生效"了，这会让开发期联调看到的行为和真实后端不一致。
 */
export async function setNodeLogLevel(
  id: string,
  input: NodeLogLevelInput,
): Promise<NodeLogLevelResult> {
  await delay(150)
  const index = nodes.findIndex((item) => item.id === id)
  const existing = index >= 0 ? nodes[index] : undefined
  if (index < 0 || !existing) {
    throw new ApiError('节点不存在', 404, 'not_found')
  }
  nodes[index] = {
    ...existing,
    desiredLogLevel: input.logLevel,
    desiredLogShipLevel: input.logShipLevel,
    actualLogLevel: 'unknown',
    actualLogShipLevel: 'unknown',
  }
  return {
    desiredLogLevel: input.logLevel,
    desiredLogShipLevel: input.logShipLevel,
    actualLogLevel: 'unknown',
    actualLogShipLevel: 'unknown',
  }
}

export async function listRegisterCodes(centerId?: string): Promise<RegisterCode[]> {
  await delay(150)
  return registerCodes
    .filter((item) => !centerId || item.centerId === centerId)
    .map(({ codeHash: _codeHash, ...rest }) => rest)
}

function randomCode(): string {
  const bytes = new Uint8Array(24)
  crypto.getRandomValues(bytes)
  return Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join('')
}

export async function createRegisterCode(
  input: RegisterCodeCreateInput,
): Promise<RegisterCodeCreated> {
  await delay(250)
  const code = randomCode()
  const record: RegisterCode & { codeHash: string } = {
    id: `rc-${String(registerCodeSeq).padStart(4, '0')}`,
    centerId: input.centerId,
    presetName: input.presetName ?? null,
    presetLabels: input.presetLabels ?? [],
    expiresAt: input.expiresAt ?? null,
    usedCount: 0,
    createdBy: 'admin',
    createdAt: new Date().toISOString(),
    revokedAt: null,
    codeHash: `demo-hash-${registerCodeSeq}`,
  }
  registerCodeSeq += 1
  registerCodes = [record, ...registerCodes]
  const { codeHash: _codeHash, ...rest } = record
  return { ...rest, code }
}

export async function revokeRegisterCode(id: string): Promise<void> {
  await delay(150)
  const index = registerCodes.findIndex((item) => item.id === id)
  const existing = index >= 0 ? registerCodes[index] : undefined
  if (index < 0 || !existing) {
    throw new ApiError('注册码不存在', 404, 'not_found')
  }
  registerCodes[index] = { ...existing, revokedAt: new Date().toISOString() }
}
