import type { DashboardOverview } from '@/api/dashboard'
import {
  MOCK_PIPELINE_BJ_TO_SH_ORDERS,
  MOCK_PIPELINE_SH_TO_BJ_USERS,
  MOCK_NODE_BJ_01,
} from './seedIds'

// Read-only demonstration fixture. Every call allocates independent results;
// production summaries are computed solely by GET /dashboard.
export async function getDashboard(): Promise<DashboardOverview> {
  const now = new Date().toISOString()
  return {
    asOf: now,
    onlineNodes: 4,
    runningPipelines: 3,
    unknownLagPipelines: 1,
    errorEvents24h: 1,
    firingAlerts: 0,
    metricFreshnessSeconds: 300,
    totalSnapshots: 2,
    lagTop: [
      {
        pipelineId: MOCK_PIPELINE_BJ_TO_SH_ORDERS,
        name: 'bj-to-sh-orders',
        runId: 1,
        lagMs: 240,
        observedAt: now,
      },
      {
        pipelineId: MOCK_PIPELINE_SH_TO_BJ_USERS,
        name: 'sh-to-bj-users',
        runId: 1,
        lagMs: 45,
        observedAt: now,
      },
    ],
    recentErrors: [
      {
        id: 'dashboard-demo-error',
        ts: now,
        type: 'node_offline',
        pipelineId: null,
        nodeId: MOCK_NODE_BJ_01,
      },
    ],
    snapshots: [
      {
        pipelineId: MOCK_PIPELINE_BJ_TO_SH_ORDERS,
        name: 'bj-to-sh-orders',
        runId: 1,
        status: 'reading',
        totalChunks: 10,
        doneChunks: 6,
        catchupRemainingTxns: null,
        updatedAt: now,
      },
      {
        pipelineId: MOCK_PIPELINE_SH_TO_BJ_USERS,
        name: 'sh-to-bj-users',
        runId: 1,
        status: 'planning',
        totalChunks: 0,
        doneChunks: 0,
        catchupRemainingTxns: null,
        updatedAt: now,
      },
    ],
  }
}
