import type { PipelineRisk } from '@/api/pipelineRisk'
import { ApiError } from '@/api/client'
import { getPipeline } from './pipelinesMock'
import { fetchMe } from './authMock'

export async function getPipelineRisk(id: string): Promise<PipelineRisk> {
  await getPipeline(id)
  return { digest: '', confirmed: false, incomplete: true, warnings: [] }
}

export async function confirmPipelineRisk(id: string, _digest: string): Promise<PipelineRisk> {
  const me = await fetchMe()
  if (!me.user || me.user.role !== 'admin')
    throw new ApiError('没有权限执行此操作', 403, 'forbidden')
  await getPipeline(id)
  throw new ApiError('演示模式无法验证线路风险', 409, 'pipeline_risk_changed')
}
