import { ETLRuleParams } from './ETLRuleParams'
import { HintForm } from '@/components/HintForm'
import { Button, Form, Input, Select, Space } from 'antd'
import { MinusCircleOutlined, PlusOutlined } from '@ant-design/icons'
import { useTranslation } from 'react-i18next'
import type { CleanseTransformRule } from '@/api/strategies'

export function TransformRuleFields() {
  const { t } = useTranslation()
  const form = Form.useFormInstance()
  const fields = 'strategies.editor.fields' as const
  return (
    <>
      <Form.List name="transforms">
        {(rules, { add, remove }) => (
          <>
            {rules.map((rule) => (
              <Space
                key={rule.key}
                wrap
                align="start"
                style={{ display: 'flex', marginBottom: 12 }}
              >
                {(['schema', 'table', 'column'] as const).map((name) => (
                  <HintForm
                    key={name}
                    name={[rule.name, name]}
                    label={t(
                      `${fields}.${name === 'schema' ? 'rowFilterSchema' : name === 'table' ? 'rowFilterTable' : 'transformColumn'}`,
                    )}
                  >
                    <Input
                      aria-label={t(
                        `${fields}.${name === 'schema' ? 'rowFilterSchema' : name === 'table' ? 'rowFilterTable' : 'transformColumn'}`,
                      )}
                      maxLength={64}
                      style={{ width: 140 }}
                    />
                  </HintForm>
                ))}
                <HintForm
                  name={[rule.name, 'execution_side']}
                  label={t(`${fields}.transformSide`)}
                >
                  <Select
                    aria-label={t(`${fields}.transformSide`)}
                    style={{ width: 140 }}
                    options={[
                      {
                        value: 'TRANSFORM_EXECUTION_SIDE_SOURCE',
                        label: t(`${fields}.transformSource`),
                      },
                      {
                        value: 'TRANSFORM_EXECUTION_SIDE_TARGET',
                        label: t(`${fields}.transformTarget`),
                      },
                      {
                        value: 'TRANSFORM_EXECUTION_SIDE_UNSPECIFIED',
                        label: t(`${fields}.transformDefault`),
                      },
                    ]}
                  />
                </HintForm>
                <HintForm name={[rule.name, 'kind']} label={t(`${fields}.transformKind`)}>
                  <Select
                    aria-label={t(`${fields}.transformKind`)}
                    style={{ width: 150 }}
                    onChange={(kind: CleanseTransformRule['kind']) =>
                      form.setFieldValue(
                        ['transforms', rule.name, 'params'],
                        kind === 'CLEANSE_TRANSFORM_KIND_MASK'
                          ? {}
                          : kind === 'CLEANSE_TRANSFORM_KIND_CONST_FILL'
                            ? { value: '', target_type: 'text' }
                            : kind === 'CLEANSE_TRANSFORM_KIND_LUA'
                              ? { script: 'return value' }
                              : kind === 'CLEANSE_TRANSFORM_KIND_PLUGIN'
                                ? { plugin: 'trim' }
                                : { target_type: 'text' },
                      )
                    }
                    options={[
                      {
                        value: 'CLEANSE_TRANSFORM_KIND_MASK',
                        label: t(`${fields}.transformMask`),
                      },
                      {
                        value: 'CLEANSE_TRANSFORM_KIND_CONST_FILL',
                        label: t(`${fields}.transformConst`),
                      },
                      {
                        value: 'CLEANSE_TRANSFORM_KIND_TYPE_CONVERT',
                        label: t(`${fields}.transformConvert`),
                      },
                      { value: 'CLEANSE_TRANSFORM_KIND_LUA', label: t(`${fields}.transformLua`) },
                      {
                        value: 'CLEANSE_TRANSFORM_KIND_PLUGIN',
                        label: t(`${fields}.transformPlugin`),
                      },
                    ]}
                  />
                </HintForm>
                <HintForm
                  noStyle
                  shouldUpdate={(previous, next) =>
                    previous.transforms?.[rule.name]?.kind !==
                    next.transforms?.[rule.name]?.kind
                  }
                >
                  {() =>
                    form.getFieldValue(['transforms', rule.name, 'kind']) ===
                    'CLEANSE_TRANSFORM_KIND_MASK' ? (
                      <>
                        {(['pattern', 'keep_prefix', 'keep_suffix'] as const).map((name) => (
                          <HintForm
                            key={name}
                            name={[rule.name, 'params', name]}
                            label={t(
                              `${fields}.${name === 'pattern' ? 'transformPattern' : name === 'keep_prefix' ? 'transformPrefix' : 'transformSuffix'}`,
                            )}
                          >
                            <Input
                              style={{ width: 140 }}
                              maxLength={name === 'pattern' ? 4096 : 4}
                            />
                          </HintForm>
                        ))}
                      </>
                    ) : ['CLEANSE_TRANSFORM_KIND_LUA', 'CLEANSE_TRANSFORM_KIND_PLUGIN'].includes(
                        form.getFieldValue(['transforms', rule.name, 'kind']),
                      ) ? (
                      <ETLRuleParams index={rule.name} kind={form.getFieldValue(['transforms', rule.name, 'kind'])} />
                    ) : (
                      <>
                        <HintForm
                          name={[rule.name, 'params', 'target_type']}
                          label={t(`${fields}.transformType`)}
                        >
                          <Select
                            style={{ width: 140 }}
                            options={['text', 'int64', 'uint64', 'float64', 'bool', 'null'].map(
                              (value) => ({ value, label: value }),
                            )}
                          />
                        </HintForm>
                        {form.getFieldValue(['transforms', rule.name, 'kind']) ===
                          'CLEANSE_TRANSFORM_KIND_CONST_FILL' && (
                          <HintForm
                            name={[rule.name, 'params', 'value']}
                            label={t(`${fields}.transformValue`)}
                          >
                            <Input maxLength={4096} style={{ width: 180 }} />
                          </HintForm>
                        )}
                      </>
                    )
                  }
                </HintForm>
                <Button
                  type="text"
                  aria-label={t(`${fields}.transformRemove`)}
                  icon={<MinusCircleOutlined />}
                  onClick={() => remove(rule.name)}
                />
              </Space>
            ))}
            <Button
              type="dashed"
              disabled={rules.length >= 128}
              icon={<PlusOutlined />}
              onClick={() =>
                add({
                  schema: '',
                  table: '',
                  column: '',
                  kind: 'CLEANSE_TRANSFORM_KIND_MASK',
                  execution_side: 'TRANSFORM_EXECUTION_SIDE_TARGET',
                  params: {},
                })
              }
            >
              {t(`${fields}.transformAdd`)}
            </Button>
          </>
        )}
      </Form.List>
    </>
  )
}
