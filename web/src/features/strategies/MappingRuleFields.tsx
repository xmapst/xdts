import { HintForm } from '@/components/HintForm'
import { HintIcon } from '@/components/HintIcon'
import { Button, Form, Input, Space } from 'antd'
import { MinusCircleOutlined, PlusOutlined } from '@ant-design/icons'
import { useTranslation } from 'react-i18next'
import type { ChangeEvent } from 'react'

export function MappingRuleFields({ incremental = true }: { incremental?: boolean }) {
  const { t } = useTranslation()
  const fields = 'strategies.editor.fields' as const
  return (
    <>
      {(['schemas', 'tables', 'columns'] as const).map((kind) => {
        const names =
          kind === 'schemas'
            ? (['source_schema', 'target_schema'] as const)
            : kind === 'tables'
              ? (['source_schema', 'source_table', 'target_table'] as const)
              : (['source_schema', 'source_table', 'source_column', 'target_column'] as const)
        return (
          <Form.List key={kind} name={['mappings', kind]}>
            {(entries, { add, remove }) => (
              <>
                {entries.map((entry) => (
                  <Space
                    key={entry.key}
                    wrap
                    align="start"
                    style={{ display: 'flex', marginBottom: 12 }}
                  >
                    {names.map((name) => (
                      <HintForm
                        key={name}
                        name={[entry.name, name]}
                        label={t(`${fields}.mapping_${name}`)}
                      >
                        <Input
                          aria-label={t(`${fields}.mapping_${name}`)}
                          style={{ width: 160 }}
                          maxLength={64}
                        />
                      </HintForm>
                    ))}
                    <Button
                      type="text"
                      aria-label={t(`${fields}.mappingRemove`)}
                      icon={<MinusCircleOutlined />}
                      onClick={() => remove(entry.name)}
                    />
                  </Space>
                ))}
                <span className="section-title" style={{ marginBottom: 8 }}>
                  <Button
                    type="dashed"
                    icon={<PlusOutlined />}
                    onClick={() => add(Object.fromEntries(names.map((name) => [name, ''])))}
                  >
                    {t(`${fields}.mappingAdd_${kind}`)}
                  </Button>
                  <HintIcon content={t(`${fields}.mappingHint`)} />
                </span>
              </>
            )}
          </Form.List>
        )
      })}
      {incremental && (
        <>
          <Space wrap align="start">
            {(['rows_per_sec', 'bytes_per_sec'] as const).map((name) => (
              <HintForm
                key={name}
                name={['incremental_rate_limit', name]}
                label={t(`${fields}.incrementalRate_${name}`)}
                extra={t(`${fields}.incrementalRateHint`)}
                getValueFromEvent={(event: ChangeEvent<HTMLInputElement>) =>
                  event.target.value === '' ? undefined : event.target.value
                }
              >
                <Input
                  inputMode="numeric"
                  aria-label={t(`${fields}.incrementalRate_${name}`)}
                  style={{ width: 190 }}
                  maxLength={20}
                />
              </HintForm>
            ))}
          </Space>
        </>
      )}
    </>
  )
}
