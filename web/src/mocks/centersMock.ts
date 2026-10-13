import { validateResourceName } from './resourceNames'
// 中心域的开发期 mock：让 `npm run dev` 不接真实后端时也能走通中心 CRUD、
// 来源身份集合、巡检节点指派的完整交互，并演示
//
// MGR-CTR-01（编码唯一）、MGR-CTR-02（UUID 全局唯一）、MGR-CTR-04
// （删除前校验无归属节点）三条后端校验规则的错误提示。
// 仅在 isApiMockEnabled() 为 true 时被调用（见 web/src/api/centers.ts）。
import { ApiError } from '@/api/client'
import type {
  CenterBrokerInput,
  CenterInput,
  CenterSummary,
  CenterUpdateInput,
  OriginUuid,
  OriginUuidInput,
} from '@/api/centers'
import { MOCK_CENTER_BJ, MOCK_CENTER_MGR, MOCK_CENTER_SH, MOCK_NODE_BJ_01 } from './seedIds'
import { countNodesByCenter } from './nodesMock'
import { countEndpointsByCenter } from './endpointsMock'

/**
 * 本域 mock 写操作镜像的后端写入方（**就地声明，不设中心白名单**）。
 * 由 `web/src/mocks/backendWriteParity.test.ts` 逐条核对可达性，双向失败。
 *
 * 中心与来源身份集合六个写操作在 internal/manager/http/centers.go 都有对应 repo 调用。
 */
export const backendWriteParity = {
  liveRepoWrites: [
    'CreateCenter',
    'UpdateCenter',
    'DeleteCenter',
    'CreateCenterOriginUUID',
    'DeleteCenterOriginUUID',
    'SetCenterInspectorNode',
  ],
  note: '见上方注释',
} as const

interface MockCenter extends CenterSummary {
  mqMgmtPassword: string | null
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

let centers: MockCenter[] = [
  {
    id: MOCK_CENTER_MGR,
    code: 'MGR',
    name: '管理中心',
    region: '华北',
    description: '承载 Manager 与管理库的中心',
    isManagerCenter: true,
    inspectorNodeId: null,
    mqMgmtUrl: null,
    mqMgmtUser: null,
    mqMgmtPassword: null,
    originUuidCount: 1,
    createdAt: '2026-07-01T00:00:00Z',
  },
  {
    id: MOCK_CENTER_BJ,
    code: 'BJ',
    name: '北京中心',
    region: '华北',
    description: null,
    isManagerCenter: false,
    inspectorNodeId: MOCK_NODE_BJ_01,
    mqMgmtUrl: 'https://bj-mq.internal:15672',
    mqMgmtUser: 'inspector',
    mqMgmtPassword: 'demo-only-not-real',
    originUuidCount: 2,
    createdAt: '2026-07-02T00:00:00Z',
  },
  {
    id: MOCK_CENTER_SH,
    code: 'SH',
    name: '上海中心',
    region: '华东',
    description: null,
    isManagerCenter: false,
    inspectorNodeId: null,
    mqMgmtUrl: null,
    mqMgmtUser: null,
    mqMgmtPassword: null,
    originUuidCount: 1,
    createdAt: '2026-07-03T00:00:00Z',
  },
]

let originUuids: OriginUuid[] = [
  {
    id: 'ou-0001',
    centerId: MOCK_CENTER_MGR,
    uuid: '11111111-1111-1111-1111-111111111111',
    kind: 'server',
    refId: 'ds-mgr-1',
    createdAt: '2026-07-01T00:10:00Z',
  },
  {
    id: 'ou-0002',
    centerId: MOCK_CENTER_BJ,
    uuid: '22222222-2222-2222-2222-222222222222',
    kind: 'server',
    refId: 'ds-bj-1',
    createdAt: '2026-07-02T00:10:00Z',
  },
  {
    id: 'ou-0003',
    centerId: MOCK_CENTER_BJ,
    uuid: '33333333-3333-3333-3333-333333333333',
    kind: 'manual',
    refId: null,
    createdAt: '2026-07-10T02:00:00Z',
  },
  {
    id: 'ou-0004',
    centerId: MOCK_CENTER_SH,
    uuid: '44444444-4444-4444-4444-444444444444',
    kind: 'group',
    refId: 'ds-sh-mgr-group',
    createdAt: '2026-07-03T00:10:00Z',
  },
]
let originUuidSeq = 5

function toPublic(center: MockCenter): CenterSummary {
  const { mqMgmtPassword: _mqMgmtPassword, ...rest } = center
  return rest
}

function recalcOriginUuidCount(centerId: string) {
  const count = originUuids.filter((item) => item.uuid && item.centerId === centerId).length
  const index = centers.findIndex((item) => item.id === centerId)
  const existing = index >= 0 ? centers[index] : undefined
  if (index >= 0 && existing) {
    centers[index] = { ...existing, originUuidCount: count }
  }
}

export async function listCenters(): Promise<CenterSummary[]> {
  await delay(200)
  return centers.map(toPublic)
}

export async function getCenter(id: string): Promise<CenterSummary> {
  await delay(150)
  const center = centers.find((item) => item.id === id)
  if (!center) {
    throw new ApiError('中心不存在', 404, 'not_found')
  }
  return toPublic(center)
}

export async function createCenter(input: CenterInput): Promise<CenterSummary> {
  await delay(250)
  const code = input.code.trim()
  if (centers.some((item) => item.code.toLowerCase() === code.toLowerCase())) {
    throw new ApiError(`编码「${code}」已存在，中心编码必须全局唯一`, 409, 'conflict')
  }
  const center: MockCenter = {
    id: `center-${crypto.randomUUID().slice(0, 8)}`,
    code,
    name: validateResourceName(input.name, centers, 'center_name_conflict'),
    region: input.region ?? null,
    description: input.description ?? null,
    isManagerCenter: input.isManagerCenter ?? false,
    inspectorNodeId: null,
    mqMgmtUrl: null,
    mqMgmtUser: null,
    mqMgmtPassword: null,
    originUuidCount: 0,
    createdAt: new Date().toISOString(),
  }
  centers = [...centers, center]
  return toPublic(center)
}

export async function updateCenter(
  id: string,
  input: CenterUpdateInput & Partial<CenterBrokerInput>,
): Promise<CenterSummary> {
  await delay(250)
  const index = centers.findIndex((item) => item.id === id)
  const existing = index >= 0 ? centers[index] : undefined
  if (index < 0 || !existing) {
    throw new ApiError('中心不存在', 404, 'not_found')
  }
  if (input.code && input.code.trim().toLowerCase() !== existing.code.toLowerCase()) {
    const conflict = centers.some(
      (item) => item.id !== id && item.code.toLowerCase() === input.code?.trim().toLowerCase(),
    )
    if (conflict) {
      throw new ApiError(`编码「${input.code}」已存在，中心编码必须全局唯一`, 409, 'conflict')
    }
  }
  const updated: MockCenter = {
    ...existing,
    code: input.code?.trim() ?? existing.code,
    name:
      input.name === undefined
        ? existing.name
        : validateResourceName(input.name, centers, 'center_name_conflict', id),
    region: input.region !== undefined ? input.region : existing.region,
    description: input.description !== undefined ? input.description : existing.description,
    isManagerCenter: input.isManagerCenter ?? existing.isManagerCenter,
    mqMgmtUrl: input.mqMgmtUrl !== undefined ? input.mqMgmtUrl : existing.mqMgmtUrl,
    mqMgmtUser: input.mqMgmtUser !== undefined ? input.mqMgmtUser : existing.mqMgmtUser,
    mqMgmtPassword:
      input.mqMgmtPassword !== undefined ? input.mqMgmtPassword : existing.mqMgmtPassword,
  }
  centers[index] = updated
  return toPublic(updated)
}

export async function deleteCenter(id: string): Promise<void> {
  await delay(250)
  const existing = centers.find((item) => item.id === id)
  if (!existing) {
    throw new ApiError('中心不存在', 404, 'not_found')
  }

  const nodeCount = countNodesByCenter(id)
  if (nodeCount > 0) {
    throw new ApiError(
      `无法删除：该中心仍有 ${nodeCount} 个归属节点，请先迁移或注销这些节点`,
      409,
      'center_in_use',
    )
  }
  const endpointCount = countEndpointsByCenter(id)
  if (endpointCount > 0) {
    throw new ApiError(
      `无法删除：该中心仍有 ${endpointCount} 个归属端点，请先迁移或删除这些端点`,
      409,
      'center_in_use',
    )
  }
  centers = centers.filter((item) => item.id !== id)
  originUuids = originUuids.filter((item) => item.centerId !== id)
}

export async function listOriginUuids(centerId: string): Promise<OriginUuid[]> {
  await delay(150)
  return Promise.all(
    originUuids
      .filter((item) => item.centerId === centerId)
      .map(async (item) => {
        if (!item.refId) return { ...item }
        try {
          if (item.kind === 'server' || item.kind === 'group') {
            const endpoint = await (await import('./endpointsMock')).getEndpoint(item.refId)
            if (endpoint.centerId === centerId)
              return { ...item, refName: endpoint.name, refType: 'endpoint' as const }
          } else if (item.kind === 'synthetic') {
            const pipeline = await (await import('./pipelinesMock')).getPipeline(item.refId)
            return { ...item, refName: pipeline.name, refType: 'pipeline' as const }
          }
        } catch (error) {
          if (!(error instanceof ApiError && error.status === 404)) throw error
        }
        return { ...item }
      }),
  )
}

export async function addOriginUuid(
  centerId: string,
  input: OriginUuidInput,
): Promise<OriginUuid> {
  await delay(200)
  const uuid = input.uuid.trim()
  if (originUuids.some((item) => item.uuid.toLowerCase() === uuid.toLowerCase())) {
    throw new ApiError(`UUID「${uuid}」已登记（来源身份在全部中心范围内唯一）`, 409, 'conflict')
  }
  const record: OriginUuid = {
    id: `ou-${String(originUuidSeq).padStart(4, '0')}`,
    centerId,
    uuid,
    kind: 'manual',
    refId: null,
    createdAt: new Date().toISOString(),
  }
  originUuidSeq += 1
  originUuids = [...originUuids, record]
  recalcOriginUuidCount(centerId)
  return record
}

export async function deleteOriginUuid(centerId: string, uuidId: string): Promise<void> {
  await delay(150)
  const existing = originUuids.find((item) => item.id === uuidId && item.centerId === centerId)
  if (!existing) {
    throw new ApiError('来源身份记录不存在', 404, 'not_found')
  }
  originUuids = originUuids.filter((item) => item.id !== uuidId)
  recalcOriginUuidCount(centerId)
}

export async function setInspectorNode(centerId: string, nodeId: string | null): Promise<void> {
  await delay(200)
  const index = centers.findIndex((item) => item.id === centerId)
  const existing = index >= 0 ? centers[index] : undefined
  if (index < 0 || !existing) {
    throw new ApiError('中心不存在', 404, 'not_found')
  }
  centers[index] = { ...existing, inspectorNodeId: nodeId }
}
