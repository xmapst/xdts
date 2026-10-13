import { validateResourceName } from './resourceNames'
// 数据端点域的开发期 mock：让 `npm run dev` 在无法访问真实后端时也能走通
// 数据端点 CRUD、「测试连接」异步探测、库表浏览的完整交互，并演示
// MGR-DS-02（探测节点必须同中心在线，跨中心 / 离线时后端拒绝）、MGR-DS-05
// （被 pipeline 引用不可删除）两条后端校验规则的真实错误提示，以及
// 阻断 / 推荐 / 警告 / 提示四级参数展示的三种典型探测结果（正常、参数不达标、
// 连接失败）。
// 仅在 isApiMockEnabled() 为 true 时被调用（见 web/src/api/endpoints.ts）。
import { ApiError } from '@/api/client'
import type {
  CatalogEntry,
  EndpointInput,
  EndpointListQuery,
  EndpointSummary,
  EndpointUpdateResult,
  EndpointUpdateInput,
  ProbeResult,
} from '@/api/endpoints'
import type { Paginated } from '@/api/types'
import {
  MOCK_CENTER_BJ,
  MOCK_CENTER_SH,
  MOCK_DE_BJ_ORDERS,
  MOCK_DE_BJ_ORDERS_UNPROBED,
  MOCK_DE_SH_ORDERS,
} from './seedIds'
import { getNodeForProbe } from './nodesMock'
import { listPipelinesByEndpoint } from './pipelinesMock'

/**
 * 本域 mock 写操作镜像的后端写入方（**就地声明，不设中心白名单**）。
 * 由 `web/src/mocks/backendWriteParity.test.ts` 逐条核对可达性，双向失败。
 *
 * 三个写操作在 internal/manager/http/endpoints.go 都有对应 repo 调用。
 */
export const backendWriteParity = {
  liveRepoWrites: ['CreateEndpoint', 'UpdateEndpoint', 'DeleteEndpoint'],
  note: '见上方注释',
} as const

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

const HEALTHY_PROBE_RESULT: ProbeResult = {
  reachable: true,
  error: null,
  version: '8.0.36',
  serverUuid: '55555555-5555-5555-5555-555555555555',

  lowerCaseTableNames: 'SENSITIVE',
  gtidMode: 'ON',
  enforceGtidConsistency: 'ON',
  logBin: 'ON',
  binlogFormat: 'ROW',
  binlogRowImage: 'FULL',
  binlogRowMetadata: 'FULL',
  binlogExpireLogsSeconds: 604_800,
  binlogRowValueOptions: '',
  binlogTransactionCompression: 'OFF',
  grants: [
    "GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'xdts_sync'@'%'",
    "GRANT SELECT ON `orders_db`.* TO 'xdts_sync'@'%'",
  ],
}

/** 演示 阻断级参数不达标：STATEMENT 格式 binlog 无法按行解析。 */
const BLOCKED_PROBE_RESULT: ProbeResult = {
  reachable: true,
  error: null,
  version: '5.7.42',
  serverUuid: '66666666-6666-6666-6666-666666666666',
  lowerCaseTableNames: 'SENSITIVE',
  gtidMode: 'OFF',
  enforceGtidConsistency: 'OFF',
  logBin: 'ON',
  binlogFormat: 'STATEMENT',
  binlogRowImage: 'MINIMAL',
  binlogRowMetadata: 'NONE',
  binlogExpireLogsSeconds: 259_200,
  binlogRowValueOptions: '',
  binlogTransactionCompression: 'OFF',
  grants: ["GRANT SELECT ON `orders_db`.* TO 'xdts_sync'@'%'"],
}

interface MockEndpoint extends EndpointSummary {
  password: string
}

function seedEndpoints(): MockEndpoint[] {
  return [
    {
      id: MOCK_DE_BJ_ORDERS,
      name: 'bj-orders-source',
      centerId: MOCK_CENTER_BJ,
      host: '10.1.2.10',
      port: 3306,
      username: 'xdts_sync',
      password: 'demo-only-not-real',
      tls: null,
      params: null,
      version: HEALTHY_PROBE_RESULT.version ?? '',
      serverUuid: HEALTHY_PROBE_RESULT.serverUuid,
      lowerCaseTableNames: HEALTHY_PROBE_RESULT.lowerCaseTableNames ?? '',
      probeNodeId: null,
      lastProbeAt: new Date(Date.now() - 2 * 3600_000).toISOString(),
      lastProbeResult: HEALTHY_PROBE_RESULT,
      createdAt: '2026-08-10T02:00:00Z',
    },
    {
      id: MOCK_DE_BJ_ORDERS_UNPROBED,
      name: 'bj-orders-target',
      centerId: MOCK_CENTER_BJ,
      host: '10.1.2.20',
      port: 3306,
      username: 'xdts_sync',
      password: 'demo-only-not-real',
      tls: null,
      params: null,
      version: '',
      serverUuid: null,
      lowerCaseTableNames: '',
      probeNodeId: null,
      lastProbeAt: null,
      lastProbeResult: null,
      createdAt: '2026-08-12T02:00:00Z',
    },
    {
      id: MOCK_DE_SH_ORDERS,
      name: 'sh-orders-legacy',
      centerId: MOCK_CENTER_SH,
      host: '10.2.2.10',
      port: 3306,
      username: 'xdts_sync',
      password: 'demo-only-not-real',
      tls: null,
      params: null,
      version: BLOCKED_PROBE_RESULT.version ?? '',
      serverUuid: BLOCKED_PROBE_RESULT.serverUuid,
      lowerCaseTableNames: BLOCKED_PROBE_RESULT.lowerCaseTableNames ?? '',
      probeNodeId: null,
      lastProbeAt: new Date(Date.now() - 30 * 86_400_000).toISOString(),
      lastProbeResult: BLOCKED_PROBE_RESULT,
      createdAt: '2026-07-20T02:00:00Z',
    },
  ]
}

let endpoints: MockEndpoint[] = seedEndpoints()
let seq = 4

/* 演示 MGR-DS-05：数据端点被引用时禁止删除。措辞对齐已经落地的
`internal/manager/repo/endpoint.go` `DeleteEndpoint`——那里的校验只
给出「仍有 N 条 pipeline 引用」的计数，不会列出具体 pipeline 名称，所以这里
同样只保留计数，不在 mock 里编造一份「引用它的 pipeline 名单」去误导界面能
展示比后端更详细的信息。 */
const referencedByPipelineCount: Record<string, number> = {
  [MOCK_DE_BJ_ORDERS]: 7,
  [MOCK_DE_SH_ORDERS]: 6,
  [MOCK_DE_BJ_ORDERS_UNPROBED]: 1,
}

/** 供 centersMock 校验「删除中心前必须无归属数据端点」（MGR-CTR-04）。 */
export function countEndpointsByCenter(centerId: string): number {
  return endpoints.filter((item) => item.centerId === centerId).length
}

function toPublic(de: MockEndpoint): EndpointSummary {
  const { password: _password, ...rest } = de
  return rest
}

export async function listEndpoints(
  query: EndpointListQuery,
): Promise<Paginated<EndpointSummary>> {
  await delay(200)
  let filtered = endpoints.filter((item) => {
    if (query.centerId && item.centerId !== query.centerId) {
      return false
    }
    if (query.keyword) {
      const keyword = query.keyword.trim().toLowerCase()
      if (
        keyword.length > 0 &&
        !item.name.toLowerCase().includes(keyword) &&
        !item.host.toLowerCase().includes(keyword)
      ) {
        return false
      }
    }
    return true
  })
  const total = filtered.length
  const start = (query.page - 1) * query.pageSize
  filtered = filtered.slice(start, start + query.pageSize)
  return { items: filtered.map(toPublic), total, page: query.page, pageSize: query.pageSize }
}

export async function getEndpoint(id: string): Promise<EndpointSummary> {
  await delay(150)
  const de = endpoints.find((item) => item.id === id)
  if (!de) {
    throw new ApiError('端点不存在', 404, 'not_found')
  }
  return toPublic(de)
}

export async function createEndpoint(input: EndpointInput): Promise<EndpointSummary> {
  await delay(250)
  const record: MockEndpoint = {
    id: `10000000-0000-4000-8000-${String(seq).padStart(12, '0')}`,
    name: validateResourceName(input.name, endpoints, 'endpoint_name_conflict'),
    centerId: input.centerId,
    host: input.host,
    port: input.port,
    username: input.username,
    password: input.password,
    tls: input.tls ?? null,
    params: input.params ?? null,
    version: '',
    serverUuid: null,
    lowerCaseTableNames: '',
    probeNodeId: null,
    lastProbeAt: null,
    lastProbeResult: null,
    createdAt: new Date().toISOString(),
  }
  seq += 1
  endpoints = [...endpoints, record]
  return toPublic(record)
}

/* The mock returns a complete affected-pipeline list. Component tests supply affectedUnknown to cover failed server lookups. */
export async function updateEndpoint(
  id: string,
  input: EndpointUpdateInput,
): Promise<EndpointUpdateResult> {
  await delay(250)
  const index = endpoints.findIndex((item) => item.id === id)
  const existing = index >= 0 ? endpoints[index] : undefined
  if (index < 0 || !existing) {
    throw new ApiError('端点不存在', 404, 'not_found')
  }
  const updated: MockEndpoint = {
    ...existing,
    name:
      input.name === undefined
        ? existing.name
        : validateResourceName(input.name, endpoints, 'endpoint_name_conflict', id),
    host: input.host ?? existing.host,
    port: input.port ?? existing.port,
    username: input.username ?? existing.username,
    password: input.password ? input.password : existing.password,
    tls: input.tls !== undefined ? input.tls : existing.tls,
    params: input.params !== undefined ? input.params : existing.params,
  }
  endpoints[index] = updated
  return {
    ...toPublic(updated),
    // 按端点取，不按角色——自环线路两侧引用同一个端点时只算一条，
    // 口径见 pipelinesMock.listPipelinesByEndpoint 的注释。
    affectedPipelines: listPipelinesByEndpoint(id).map((p) => ({
      id: p.id,
      name: p.name,
      desiredState: p.desiredState,
      sourceActualState: p.sourceActualState,
      targetActualState: p.targetActualState,
    })),
    affectedUnknown: false,
  }
}

export async function deleteEndpoint(id: string): Promise<void> {
  await delay(250)
  const existing = endpoints.find((item) => item.id === id)
  if (!existing) {
    throw new ApiError('端点不存在', 404, 'not_found')
  }
  const pipelineCount = referencedByPipelineCount[id] ?? 0
  if (pipelineCount > 0) {
    throw new ApiError(
      `端点被 pipeline 引用，不能删除：仍有 ${pipelineCount} 条 pipeline 引用该端点（含待清理未完成的）`,
      409,
      'endpoint_in_use',
    )
  }
  endpoints = endpoints.filter((item) => item.id !== id)
}

/**
 * 模拟异步探测：校验节点同中心 + 在线（MGR-DS-02，后端的硬约束，即便前端下拉
 * 已经预过滤，这里仍然做一遍，模拟「万一传进来一个不合法 nodeId」时后端会
 * 如实拒绝，而不是前端自己吞掉）；校验通过后延迟一段时间再写回
 * last_probe_at / last_probe_result，调用方通过轮询 getEndpoint 观察结果。
 */
let nextProbeTask = 0

export async function probeEndpoint(id: string, nodeId: string): Promise<{ taskId: string }> {
  const index = endpoints.findIndex((item) => item.id === id)
  const existing = index >= 0 ? endpoints[index] : undefined
  if (index < 0 || !existing) {
    throw new ApiError('端点不存在', 404, 'not_found')
  }
  const node = getNodeForProbe(nodeId)
  if (!node) {
    throw new ApiError('探测节点不存在', 404, 'node_not_found')
  }
  if (node.centerId !== existing.centerId) {
    throw new ApiError(
      '探测节点必须与端点同中心：所选节点所属中心与该端点不一致，已拒绝派发',
      409,
      'cross_center_rejected',
    )
  }
  if (node.status !== 'online') {
    throw new ApiError('探测节点当前离线，无法派发探测任务', 409, 'node_offline')
  }
  await delay(200)
  const taskId = `mock-probe-${++nextProbeTask}`
  // 派发已被接受（HTTP 层面成功返回）；真正的探测结果异步写回，模拟节点执行
  // 耗时，调用方需要轮询 getEndpoint 才能看到 last_probe_at 前进。
  void (async () => {
    await delay(1500)
    const current = endpoints.findIndex((item) => item.id === id)
    const currentRecord = current >= 0 ? endpoints[current] : undefined
    if (current < 0 || !currentRecord) {
      return
    }
    let result: ProbeResult = HEALTHY_PROBE_RESULT
    if (id === MOCK_DE_SH_ORDERS) {
      result = BLOCKED_PROBE_RESULT
    } else if (id === MOCK_DE_BJ_ORDERS_UNPROBED) {
      result = {
        reachable: false,
        error: `连接超时：无法连接到 ${existing.host}:${existing.port}`,
        version: null,
        serverUuid: null,
        lowerCaseTableNames: null,
        gtidMode: null,
        enforceGtidConsistency: null,
        logBin: null,
        binlogFormat: null,
        binlogRowImage: null,
        binlogRowMetadata: null,
        binlogExpireLogsSeconds: null,
        binlogRowValueOptions: null,
        binlogTransactionCompression: null,
        grants: [],
      }
    }
    endpoints[current] = {
      ...currentRecord,
      probeNodeId: nodeId,
      lastProbeAt: new Date().toISOString(),
      lastProbeResult: { ...result, taskId },
      version: result.version ?? currentRecord.version,
      serverUuid: result.serverUuid ?? currentRecord.serverUuid,
      lowerCaseTableNames: result.lowerCaseTableNames ?? currentRecord.lowerCaseTableNames,
    }
  })()
  return { taskId }
}

const DEMO_CATALOG: CatalogEntry[] = [
  {
    schema: 'orders_db',
    table: 'orders',
    estimatedRows: 12_450_000,
    hasPrimaryKey: true,
    hasUniqueKey: true,
  },
  {
    schema: 'orders_db',
    table: 'order_items',
    estimatedRows: 38_200_000,
    hasPrimaryKey: true,
    hasUniqueKey: false,
  },
  {
    schema: 'orders_db',
    table: 'refund_log',
    estimatedRows: 512_000,
    hasPrimaryKey: false,
    hasUniqueKey: true,
  },
]

export async function getEndpointCatalog(id: string): Promise<CatalogEntry[]> {
  await delay(300)
  const existing = endpoints.find((item) => item.id === id)
  if (!existing) {
    throw new ApiError('端点不存在', 404, 'not_found')
  }
  if (!existing.lastProbeResult?.reachable) {
    throw new ApiError(
      '尚未成功探测该端点，无法浏览库表元数据，请先测试连接',
      409,
      'not_probed',
    )
  }
  return DEMO_CATALOG
}
