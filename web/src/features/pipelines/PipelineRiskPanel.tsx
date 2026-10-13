import { useHintFeedback } from '@/components/hintFeedback'
import { HintIcon } from '@/components/HintIcon'
import { Button, Space } from 'antd'
import { useState } from 'react'
import { ActionDrawer } from '@/components/ActionDrawer'
import { useTranslation } from 'react-i18next'
import { usePipelineRiskQuery, useConfirmPipelineRiskMutation } from '@/api/pipelineRisk'

export function PipelineRiskPanel({
  pipelineId,
  canWrite,
}: {
  pipelineId: string
  canWrite: boolean
}) {
  const { t } = useTranslation()
  const { message } = useHintFeedback()
  const [confirmation, setConfirmation] = useState<string>()
  const query = usePipelineRiskQuery(pipelineId)
  const confirm = useConfirmPipelineRiskMutation(pipelineId)
  const result = query.data
  return (
    <Space wrap size={8}>
      {query.error && <HintIcon type="error" content={query.error.message} />}
      {result && (
        <>
          <HintIcon
            type={result.incomplete || result.warnings.length ? 'warning' : 'success'}
            content={
              <>
                <div>
                  {t(
                    result.incomplete
                      ? 'pipelines.risk.analysisIncomplete'
                      : result.warnings.length
                        ? 'pipelines.risk.hasWarnings'
                        : 'pipelines.risk.noWarnings',
                  )}
                </div>
                {!result.incomplete && result.warnings.length > 0 && result.confirmed && (
                  <div>{t('pipelines.risk.confirmed')}</div>
                )}
                {result.warnings.map((warning, index) => (
                  <div key={index}>
                    <strong>{t(`pipelines.risk.warningKinds.${warning.kind}`)}</strong>
                    <div>{warning.path.centers.join(' → ')}</div>
                    <div>{warning.path.pipelines.join(' → ')}</div>
                    {warning.previous && (
                      <div>
                        {t('pipelines.risk.firstPath')}:{' '}
                        {warning.previous.centers.join(' → ')} /{' '}
                        {warning.previous.pipelines.join(' → ')}
                      </div>
                    )}
                  </div>
                ))}
              </>
            }
          />
          {!result.incomplete &&
            result.warnings.length > 0 &&
            !result.confirmed &&
            canWrite && (
              <Button
                loading={confirm.isPending}
                onClick={() => setConfirmation(result.digest)}
              >
                {t('pipelines.risk.confirmWarnings')}
              </Button>
            )}
        </>
      )}
      <ActionDrawer
        open={confirmation !== undefined}
        title={
          <span className="section-title">
            {t('pipelines.risk.confirmTitle')}
            <HintIcon content={t('pipelines.risk.confirmDescription')} />
          </span>
        }
        onClose={() => setConfirmation(undefined)}
        confirmLoading={confirm.isPending}
        onConfirm={async () => {
          if (!confirmation) return
          try {
            await confirm.mutateAsync(confirmation)
            setConfirmation(undefined)
          } catch (error) {
            void message.error(
              error instanceof Error ? error.message : t('pipelines.risk.confirmFailed'),
            )
          }
        }}
        destroyOnHidden
      />
    </Space>
  )
}
