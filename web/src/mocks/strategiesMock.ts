// 策略域的开发期 mock：让 `npm run dev` 在本地不接真实后端时也能走通策略
// CRUD、版本创建（=「编辑」）、版本删除、版本对比的完整交互，并演示
// MGR-STR-04（被 pipeline 引用不可删除，措辞对齐已落地的
// internal/manager/repo/strategy.go DeleteStrategy——只给引用计数）。
// 仅在 isApiMockEnabled() 为 true 时被调用（见 web/src/api/strategies.ts）。
import { ApiError } from '@/api/client'
import type {
  StrategyDiffEntry,
  StrategyInput,
  StrategyRules,
  StrategySummary,
  StrategyUpdateInput,
  StrategyVersionSummary,
} from '@/api/strategies'
import { createDefaultStrategyRules } from '@/api/strategies'
import {
  MOCK_STRATEGY_ORDERS,
  MOCK_STRATEGY_SETTINGS_MIRROR,
  MOCK_STRATEGY_USERS_ONEWAY,
} from './seedIds'
import { diffRules } from '@/features/strategies/ruleDiff'
import { toWireRules, validateWireRules } from '@/api/strategyRulesWire'

/**
 * 本域 mock 写操作镜像的后端写入方（**就地声明，不设中心白名单**）。
 * 由 `web/src/mocks/backendWriteParity.test.ts` 逐条核对可达性，双向失败。
 *
 * 五个写操作在 internal/manager/http/strategies.go 都有对应 repo 调用。
 */
export const backendWriteParity = {
  liveRepoWrites: [
    'CreateStrategy',
    'UpdateStrategyMeta',
    'DeleteStrategy',
    'CreateStrategyVersion',
    'DeleteStrategyVersion',
  ],
  note: '见上方注释',
} as const

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function defaultRules(): StrategyRules {
  const base = createDefaultStrategyRules()
  return {
    ...base,
    object_filters: [
      {
        action: 'FILTER_ACTION_INCLUDE',
        schema_match: 'MATCH_TYPE_EXACT',
        schema_pattern: 'orders_db',
        table_match: 'MATCH_TYPE_UNSPECIFIED',
        table_pattern: '',
      },
    ],
  }
}

/* 种子数据存的必须是线格式，和服务端落库后的形状一致。

过一次 `toWireRules` 而不是手写线格式，有两个理由：一是种子与出站映射共用
同一个真源，映射改了种子跟着改；二是它顺带断言"种子本身是可表示的"——
哪天有人往种子里放一个协议装不下的值（多表 LWW 之类），这里当场抛，而不是
等到某个用例以一种看不出因果的方式变红。 */
function seedWire(flat: StrategyRules): unknown {
  const mapped = toWireRules(flat)
  if (!mapped.ok) {
    throw new Error(
      `mock 种子数据无法映射成线格式：${mapped.blockers.map((b) => b.message).join('；')}`,
    )
  }
  return mapped.wire
}

interface MockStrategy extends StrategySummary {
  versions: StrategyVersionSummary[]
}

function seedStrategies(): MockStrategy[] {
  const v1Rules = defaultRules()
  const v2Rules: StrategyRules = {
    ...v1Rules,
    operation_filter: { ...v1Rules.operation_filter, ddl: true },
    ddlPolicy: { ...v1Rules.ddlPolicy, mode: 'sync' },
    errorPolicy: { ...v1Rules.errorPolicy, maxRetries: 8 },
  }
  return [
    {
      id: MOCK_STRATEGY_ORDERS,
      name: 'orders-sync',
      description: '订单库北京 → 上海同步策略',
      currentVersion: 2,
      createdAt: '2026-08-01T00:00:00Z',
      updatedAt: '2026-08-15T00:00:00Z',
      versions: [
        {
          id: 'sv-0001',
          strategyId: MOCK_STRATEGY_ORDERS,
          version: 1,
          rules: seedWire(v1Rules),
          createdBy: 'admin',
          createdAt: '2026-08-01T00:00:00Z',
        },
        {
          id: 'sv-0002',
          strategyId: MOCK_STRATEGY_ORDERS,
          version: 2,
          rules: seedWire(v2Rules),
          createdBy: 'admin',
          createdAt: '2026-08-15T00:00:00Z',
        },
      ],
    },
    {
      id: MOCK_STRATEGY_USERS_ONEWAY,
      name: 'users-oneway',
      description: '用户库单向同步（未被任何 pipeline 引用，可演示删除）',
      currentVersion: 1,
      createdAt: '2026-08-05T00:00:00Z',
      updatedAt: '2026-08-05T00:00:00Z',
      versions: [
        {
          id: 'sv-0003',
          strategyId: MOCK_STRATEGY_USERS_ONEWAY,
          version: 1,
          rules: seedWire(defaultRules()),
          createdBy: 'admin',
          createdAt: '2026-08-05T00:00:00Z',
        },
      ],
    },
    {
      // currentVersion 特意定成 3（不是 2）：策略列表页把 currentVersion 渲染成
      // "v{n}" 文本，orders-sync 已经占用了 "v2"——如果这里也用 2，
      // web/src/features/strategies/StrategiesPage.test.tsx 现有的 `getByText('v2')` 断言会因为撞见两个
      // "v2" 而报 "Found multiple elements"（真实踩过，见测试跑出来的报错）。
      // 三个版本本身也更真实：v1 未被引用且非当前（演示删除成功），v2 只是
      // 中间版本（不需要特意演示任何一条拒绝路径，纯粹垫出版本号），v3 当前。
      id: 'strategy-settings-mirror',
      name: 'settings-mirror',
      description: '配置库镜像（v1 既非当前版本也未被引用，演示版本删除成功路径）',
      currentVersion: 3,
      createdAt: '2026-08-10T00:00:00Z',
      updatedAt: '2026-08-20T00:00:00Z',
      versions: [
        {
          id: 'sv-0004',
          strategyId: 'strategy-settings-mirror',
          version: 1,
          rules: seedWire(defaultRules()),
          createdBy: 'admin',
          createdAt: '2026-08-10T00:00:00Z',
        },
        {
          id: 'sv-0005',
          strategyId: 'strategy-settings-mirror',
          version: 2,
          rules: seedWire({
            ...defaultRules(),
            errorPolicy: { maxRetries: 3, backoffMs: 1000, skipAndLogOnNonRetryable: true },
          }),
          createdBy: 'admin',
          createdAt: '2026-08-15T00:00:00Z',
        },
        {
          id: 'sv-0006',
          strategyId: 'strategy-settings-mirror',
          version: 3,
          rules: seedWire({
            ...defaultRules(),
            errorPolicy: { maxRetries: 3, backoffMs: 1000, skipAndLogOnNonRetryable: true },
            snapshotParams: { ...defaultRules().snapshotParams, readConcurrency: 8 },
          }),
          createdBy: 'admin',
          createdAt: '2026-08-20T00:00:00Z',
        },
      ],
    },
  ]
}

let strategies: MockStrategy[] = seedStrategies()
let versionSeq = 7

/* Strategy reference counts match pipeline fixtures so deletion checks reflect actual mock dependencies. */
const referencedByPipelineCount: Record<string, number> = {
  [MOCK_STRATEGY_ORDERS]: 1,
  [MOCK_STRATEGY_SETTINGS_MIRROR]: 6,
}

/* Version reference counts are independent of whole-strategy counts; unused noncurrent versions may be deleted. */
const versionReferencedByPipelineCount: Record<string, number> = {
  [`${MOCK_STRATEGY_ORDERS}:1`]: 1,
  [`${MOCK_STRATEGY_SETTINGS_MIRROR}:2`]: 4,
}

function toPublic(s: MockStrategy): StrategySummary {
  const { versions: _versions, ...rest } = s
  return rest
}

export async function listStrategies(): Promise<StrategySummary[]> {
  await delay(200)
  return strategies.map(toPublic)
}

export async function getStrategy(id: string): Promise<StrategySummary> {
  await delay(150)
  const s = strategies.find((item) => item.id === id)
  if (!s) {
    throw new ApiError('策略不存在', 404, 'not_found')
  }
  return toPublic(s)
}

export async function createStrategy(input: StrategyInput): Promise<StrategySummary> {
  await delay(250)
  const name = input.name.trim()
  if (strategies.some((item) => item.name.toLowerCase() === name.toLowerCase())) {
    throw new ApiError(`策略名称「${name}」已存在，策略名称必须全局唯一`, 409, 'conflict')
  }
  const record: MockStrategy = {
    id: `strategy-${crypto.randomUUID().slice(0, 8)}`,
    name,
    description: input.description ?? '',
    currentVersion: 0,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    versions: [],
  }
  strategies = [...strategies, record]
  return toPublic(record)
}

export async function updateStrategyMeta(
  id: string,
  input: StrategyUpdateInput,
): Promise<StrategySummary> {
  await delay(250)
  const index = strategies.findIndex((item) => item.id === id)
  const existing = index >= 0 ? strategies[index] : undefined
  if (index < 0 || !existing) {
    throw new ApiError('策略不存在', 404, 'not_found')
  }
  if (input.name && input.name.trim().toLowerCase() !== existing.name.toLowerCase()) {
    const conflict = strategies.some(
      (item) => item.id !== id && item.name.toLowerCase() === input.name?.trim().toLowerCase(),
    )
    if (conflict) {
      throw new ApiError(`策略名称「${input.name}」已存在，策略名称必须全局唯一`, 409, 'conflict')
    }
  }
  const updated: MockStrategy = {
    ...existing,
    name: input.name?.trim() ?? existing.name,
    description: input.description !== undefined ? input.description : existing.description,
    updatedAt: new Date().toISOString(),
  }
  strategies[index] = updated
  return toPublic(updated)
}

export async function deleteStrategy(id: string): Promise<void> {
  await delay(250)
  const existing = strategies.find((item) => item.id === id)
  if (!existing) {
    throw new ApiError('策略不存在', 404, 'not_found')
  }
  const pipelineCount = referencedByPipelineCount[id] ?? 0
  if (pipelineCount > 0) {
    throw new ApiError(
      `策略被 pipeline 引用，不能删除：仍有 ${pipelineCount} 条 pipeline 引用该策略`,
      409,
      'strategy_in_use',
    )
  }
  strategies = strategies.filter((item) => item.id !== id)
}

export async function listStrategyVersions(id: string): Promise<StrategyVersionSummary[]> {
  await delay(150)
  const existing = strategies.find((item) => item.id === id)
  if (!existing) {
    throw new ApiError('策略不存在', 404, 'not_found')
  }
  return [...existing.versions].sort((a, b) => a.version - b.version)
}

/** Prepare independent rule copies; the caller commits after all validation. */
export function prepareLocalStrategyCopy(id: string, version: number, name: string) {
  const original = strategies
    .find((item) => item.id === id)
    ?.versions.find((item) => item.version === version)
  if (!original) throw new ApiError('策略版本不存在', 400, 'strategy_version_not_found')
  if (strategies.some((item) => item.name.toLowerCase() === name.toLowerCase()))
    throw new ApiError('策略名称已存在', 409, 'name_conflict')
  const rules = structuredClone(original.rules) as unknown as Record<string, unknown>
  const source =
    typeof rules.source === 'object' && rules.source !== null
      ? (rules.source as Record<string, unknown>)
      : {}
  rules.source = { ...source, origin_policy: { mode: 'ORIGIN_POLICY_MODE_LOCAL_ONLY' } }
  const rejection = validateWireRules(rules)
  if (rejection) throw new ApiError(rejection.message, 400, rejection.code)
  const now = new Date().toISOString()
  const copyID = crypto.randomUUID()
  const record: MockStrategy = {
    id: copyID,
    name,
    description: `local_only copy of ${id}`,
    currentVersion: 1,
    createdAt: now,
    updatedAt: now,
    versions: [
      {
        id: crypto.randomUUID(),
        strategyId: copyID,
        version: 1,
        rules: rules as unknown as StrategyVersionSummary['rules'],
        createdBy: 'admin',
        createdAt: now,
      },
    ],
  }
  return {
    id: copyID,
    commit: () => {
      strategies = [...strategies, record]
      referencedByPipelineCount[copyID] = 1
      versionReferencedByPipelineCount[`${copyID}:1`] = 1
    },
  }
}

/* 本函数刻意和服务端同档地拒绝非法形状。

`rules` 的静态类型刻意是 `unknown` 而不是 `StrategyRules`：mock 模拟的是
网络那一端，它看到的就是一份没有类型保证的 JSON。写成 `StrategyRules`
会让"发错形状"在编译期就不可表达，而那恰恰是这里要能发生、要被抓住的事。 */
export async function createStrategyVersion(
  id: string,
  rules: unknown,
): Promise<StrategyVersionSummary> {
  await delay(300)
  const rejection = validateWireRules(rules)
  if (rejection) {
    // 形状校验失败与服务端一样返回 invalid_rules。
    throw new ApiError(rejection.message, 400, rejection.code)
  }
  const index = strategies.findIndex((item) => item.id === id)
  const existing = index >= 0 ? strategies[index] : undefined
  if (index < 0 || !existing) {
    throw new ApiError('策略不存在', 404, 'not_found')
  }
  const nextVersion = existing.versions.reduce((max, v) => Math.max(max, v.version), 0) + 1
  const record: StrategyVersionSummary = {
    id: `sv-${String(versionSeq).padStart(4, '0')}`,
    strategyId: id,
    version: nextVersion,
    // 校验已通过 ⇒ 形状是线格式；mock 的存量只供回显，这里按既有
    // StrategyVersionSummary.rules 的类型收窄，不改那个类型的定义。
    rules: rules as StrategyVersionSummary['rules'],
    createdBy: 'admin',
    createdAt: new Date().toISOString(),
  }
  versionSeq += 1
  strategies[index] = {
    ...existing,
    versions: [...existing.versions, record],
    currentVersion: nextVersion,
    updatedAt: record.createdAt,
  }
  return record
}

/**
 * 见 web/src/api/strategies.ts 文件头注释 3：两条独立拒绝检查映射到两个错误码，
 * 分别提示；`Unscoped()` 那层含义（软删除未回收的 pipeline 也算引用）是
 * 后端仓储层的实现细节，这里只需要引用计数不为零就拒绝，不用模拟软删除
 * 状态本身。
 */
export async function deleteStrategyVersion(id: string, version: number): Promise<void> {
  await delay(250)
  const index = strategies.findIndex((item) => item.id === id)
  const existing = index >= 0 ? strategies[index] : undefined
  if (index < 0 || !existing) {
    throw new ApiError('策略不存在', 404, 'not_found')
  }
  const target = existing.versions.find((item) => item.version === version)
  if (!target) {
    throw new ApiError('版本不存在', 404, 'version_not_found')
  }
  if (existing.currentVersion === version) {
    throw new ApiError('该版本是当前生效版本，需要先发布新版本才能删除', 409, 'version_is_current')
  }
  const dependents = versionReferencedByPipelineCount[`${id}:${version}`] ?? 0
  if (dependents > 0) {
    throw new ApiError(
      `该版本被 ${dependents} 条 pipeline 引用（含软删除未回收的），不能删除`,
      409,
      'version_has_dependents',
    )
  }
  strategies[index] = {
    ...existing,
    versions: existing.versions.filter((item) => item.version !== version),
  }
}

export async function getStrategyVersionDiff(
  id: string,
  version: number,
  base?: number,
): Promise<StrategyDiffEntry[]> {
  await delay(200)
  const existing = strategies.find((item) => item.id === id)
  if (!existing) {
    throw new ApiError('策略不存在', 404, 'not_found')
  }
  const effectiveBase = base ?? version - 1
  const a = existing.versions.find((item) => item.version === effectiveBase)
  const b = existing.versions.find((item) => item.version === version)
  if (!a || !b) {
    throw new ApiError('对比的版本不存在', 404, 'version_not_found')
  }

  const entries = diffRules(a.rules, b.rules)
  return entries.sort((x, y) => (x.path < y.path ? -1 : x.path > y.path ? 1 : 0))
}
