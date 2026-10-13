import type { GeneratedPipelineRulesUpdateInput } from '@/api/generated'
import type { PipelineRules } from '@/api/pipelineRules'
import { getPipeline, readInlinePipelineRules, writeInlinePipelineRules } from './pipelinesMock'
import { listStrategyVersions } from './strategiesMock'
import { fetchMe } from './authMock'
import { ApiError } from '@/api/client'

export async function getPipelineRules(id: string): Promise<PipelineRules> {
  const pipeline = await getPipeline(id)
  const rules =
    readInlinePipelineRules(id) ??
    (await listStrategyVersions(pipeline.strategyId)).find(
      (version) => version.version === pipeline.strategyVersion,
    )?.rules ??
    {}
  return {
    configVersion: pipeline.configVersion,
    version: pipeline.strategyVersion,
    rules,
    editable: true,
    application: { source: { status: 'pending' }, target: { status: 'pending' } },
  }
}

export async function savePipelineRules(
  id: string,
  input: GeneratedPipelineRulesUpdateInput,
): Promise<PipelineRules> {
  const me = await fetchMe()
  if (!me.user || me.user.role !== 'admin')
    throw new ApiError('没有权限执行此操作', 403, 'forbidden')
  await getPipeline(id)
  writeInlinePipelineRules(id, input)
  return getPipelineRules(id)
}

export async function applyPipelineRules(
  id: string,
  input: { configVersion: number },
): Promise<PipelineRules> {
  const me = await fetchMe()
  if (!me.user || me.user.role !== 'admin')
    throw new ApiError('没有权限执行此操作', 403, 'forbidden')
  const current = await getPipelineRules(id)
  writeInlinePipelineRules(
    id,
    { configVersion: input.configVersion, rules: current.rules },
    false,
  )
  return getPipelineRules(id)
}
