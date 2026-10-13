// 开发期 mock 数据的共用 ID 常量：centersMock 与 nodesMock 都要引用同一组中心 /
// 节点 ID 才能演示「节点归属中心」「删除中心前校验归属节点」等联动场景，
// 集中定义避免两个 mock 文件各自硬编码字符串导致漂移。
export const MOCK_CENTER_MGR = 'center-mgr'
export const MOCK_CENTER_BJ = 'center-bj'
export const MOCK_CENTER_SH = 'center-sh'

export const MOCK_NODE_MGR_01 = 'node-mgr-01'
export const MOCK_NODE_BJ_01 = 'node-bj-01'
export const MOCK_NODE_BJ_02 = 'node-bj-02'
export const MOCK_NODE_SH_01 = 'node-sh-01'
export const MOCK_NODE_SH_02 = 'node-sh-02'
export const MOCK_NODE_SH_03 = 'node-sh-03'
export const MOCK_NODE_SH_04 = 'node-sh-04'

export const MOCK_DE_BJ_ORDERS = '10000000-0000-4000-8000-000000000001'
export const MOCK_DE_BJ_ORDERS_UNPROBED = '10000000-0000-4000-8000-000000000002'
export const MOCK_DE_SH_ORDERS = '10000000-0000-4000-8000-000000000003'

export const MOCK_STRATEGY_ORDERS = 'strategy-orders-sync'

export const MOCK_STRATEGY_USERS_ONEWAY = 'strategy-users-oneway'
export const MOCK_STRATEGY_SETTINGS_MIRROR = 'strategy-settings-mirror'

export const MOCK_TOPOLOGY_DEFAULT = 'topology-default'

// pipelinesMock 的种子 ID。前两个（pl-1001/pl-1002）不是新起的名字——
// nodesMock 的 `getNode()` 演示数据里早就以字面量硬编码了这两个 ID 和名称
// （节点详情「承载的 Pipeline」tab），这里把它们提升为共享常量、同时改写
// nodesMock 引用这两个常量，而不是另起一套不认识 nodesMock 既有值的新 ID——
// 否则从节点详情点进 pipeline 详情会直接 404，是本文件头注释说的那种
// "两个 mock 文件各自硬编码字符串导致漂移"的真实案例。
export const MOCK_PIPELINE_BJ_TO_SH_ORDERS = 'pl-1001'
export const MOCK_PIPELINE_SH_TO_BJ_USERS = 'pl-1002'
export const MOCK_PIPELINE_SH_TO_BJ_ORDERS_UNKNOWN = 'pl-1003'
export const MOCK_PIPELINE_BJ_TO_SH_ORDERS_ERROR = 'pl-1004'
export const MOCK_PIPELINE_SH_TO_BJ_USERS_LEGACY = 'pl-1005'

export const MOCK_PIPELINE_SH_TO_BJ_SPLIT_TERMINAL = 'pl-1006'

export const MOCK_PIPELINE_SELF_LOOP_BJ = 'pl-1007'
