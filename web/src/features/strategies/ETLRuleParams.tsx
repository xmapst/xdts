import { Form, Input, Select, Space } from 'antd'
import { useTranslation } from 'react-i18next'
import { HintForm } from '@/components/HintForm'
import { HintIcon } from '@/components/HintIcon'

export function ETLRuleParams({ index, kind }: { index: number; kind: string }) {
  const { t } = useTranslation()
  const form = Form.useFormInstance()
  const plugin = Form.useWatch(['transforms', index, 'params', 'plugin'], {
    form,
    preserve: true,
  })
  const prefix = 'strategies.editor.fields' as const
  if (kind === 'CLEANSE_TRANSFORM_KIND_LUA')
    return (
      <HintForm
        name={[index, 'params', 'script']}
        label={
          <Space size={4}>
            {t(`${prefix}.transformScript`)}
            <HintIcon content={t(`${prefix}.luaHint`)} />
          </Space>
        }
        rules={[{ required: true }]}
      >
        <Input.TextArea
          aria-label={t(`${prefix}.transformScript`)}
          maxLength={4096}
          autoSize={{ minRows: 2, maxRows: 8 }}
          style={{ width: 360, fontFamily: 'monospace' }}
        />
      </HintForm>
    )
  return (
    <>
      <HintForm name={[index, 'params', 'plugin']} label={t(`${prefix}.transformPlugin`)}>
        <Select
          style={{ width: 160 }}
          aria-label={t(`${prefix}.transformPlugin`)}
          options={['trim', 'replace', 'json_extract'].map((value) => ({
            value,
            label: value,
          }))}
          onChange={(value: string) =>
            form.setFieldValue(
              ['transforms', index, 'params'],
              value === 'replace'
                ? { plugin: value, old: '', new: '' }
                : value === 'json_extract'
                  ? { plugin: value, path: '$.value' }
                  : { plugin: value },
            )
          }
        />
      </HintForm>
      {(plugin === 'replace'
        ? ['old', 'new']
        : plugin === 'json_extract'
          ? ['path']
          : ['cutset']
      ).map((name) => (
        <HintForm
          key={name}
          name={[index, 'params', name]}
          label={t(`${prefix}.plugin_${name as 'old' | 'new' | 'path' | 'cutset'}`)}
        >
          <Input maxLength={4096} style={{ width: 180 }} />
        </HintForm>
      ))}
    </>
  )
}
