import { useHintFeedback } from '@/components/hintFeedback'
import { HintIcon } from '@/components/HintIcon'
import { useState } from 'react'
import { Button, Card, Spin, Space, Tag } from 'antd'
import { useTranslation } from 'react-i18next'
import type { PipelineSummary } from '@/api/pipelines'
import {
  usePipelineRulesQuery,
  useSavePipelineRulesMutation,
  useApplyPipelineRulesMutation,
} from '@/api/pipelineRules'
import { createDefaultStrategyRules } from '@/api/strategies'
import { fromWireRules } from '@/api/strategyRulesWire'
import { StrategyRuleEditorDrawer } from '@/features/strategies/StrategyRuleEditorDrawer'

export function PipelineRulesPanel({
  pipeline,
  canWrite,
}: {
  pipeline: PipelineSummary
  canWrite: boolean
}) {
  const { t } = useTranslation()
  const { message } = useHintFeedback()
  const [open, setOpen] = useState(false)
  const [editingVersion, setEditingVersion] = useState<number>()
  const query = usePipelineRulesQuery(pipeline.id)
  const save = useSavePipelineRulesMutation(pipeline.id)
  const apply = useApplyPipelineRulesMutation(pipeline.id)
  const editBlocked = query.data?.editable === false
  const editReason = query.data?.editBlockedReason
  const mapped = query.data
    ? fromWireRules(query.data.rules, createDefaultStrategyRules())
    : undefined
  return (
    <Card size="small" className="pipeline-fill-card">
      <Space wrap style={{ marginBottom: 12 }}>
        {query.data && (
          <span>
            {t('pipelines.rules.version')}: v{query.data.version}
          </span>
        )}
        {query.data && (
          <HintIcon
            content={
              <>
                {t('pipelines.rules.hint')} {t('pipelines.rules.version')}: v
                {query.data.version}
                <br />
                {t('pipelines.rules.dynamicHint')}
              </>
            }
          />
        )}
        {canWrite && editBlocked && (
          <HintIcon
            type="warning"
            content={t(
              `pipelines.rules.editReasons.${editReason ?? 'pipeline_not_inactive'}`,
              {
                defaultValue: t('pipelines.rules.inactive'),
              },
            )}
          />
        )}
        {query.error && <HintIcon type="error" content={query.error.message} />}
        {canWrite && (
          <Button
            disabled={!query.data || editBlocked || save.isPending || apply.isPending}
            onClick={() => {
              setEditingVersion(query.data?.configVersion)
              setOpen(true)
            }}
          >
            {t('common.edit')}
          </Button>
        )}
      </Space>
      {query.isLoading && <Spin />}
      {query.data && (
        <>
          {query.data.application && (
            <Space wrap style={{ marginBottom: 12 }}>
              {(['source', 'target'] as const).map((side) => {
                const result = query.data!.application![side]
                return (
                  <Space key={side} size={6}>
                    <Tag
                      color={
                        result.status === 'applied'
                          ? 'success'
                          : result.status === 'pending'
                            ? 'processing'
                            : 'error'
                      }
                    >
                      {t(`pipelines.side.${side}`)}:{' '}
                      {t(`pipelines.rules.application.${result.status}`)}
                    </Tag>
                    {result.message &&
                      result.status !== 'applied' &&
                      result.status !== 'pending' && (
                        <HintIcon type="error" content={result.message} />
                      )}
                  </Space>
                )
              })}
              {canWrite &&
                (query.data.application.source.status !== 'applied' ||
                  query.data.application.target.status !== 'applied') && (
                  <Button
                    size="small"
                    loading={apply.isPending}
                    disabled={save.isPending || editBlocked}
                    onClick={() => {
                      void apply
                        .mutateAsync(query.data!.configVersion)
                        .then(() => {
                          void message.success(t('pipelines.rules.saved'))
                        })
                        .catch((error: Error) => {
                          void message.error(error.message)
                        })
                    }}
                  >
                    {t('pipelines.rules.reapply')}
                  </Button>
                )}
            </Space>
          )}
          <pre className="pipeline-rule-content" aria-label={t('pipelines.rules.content')}>
            {JSON.stringify(query.data.rules, null, 2)}
          </pre>
        </>
      )}
      {mapped && query.data && (
        <StrategyRuleEditorDrawer
          open={open}
          onClose={() => setOpen(false)}
          initMode={pipeline.initMode}
          baseRules={mapped.rules}
          baseNotices={mapped.notices}
          saving={save.isPending}
          onSave={async (rules) => {
            await save.mutateAsync({
              configVersion: editingVersion ?? query.data.configVersion,
              rules,
            })
            void message.success(t('pipelines.rules.saved'))
          }}
        />
      )}
    </Card>
  )
}
