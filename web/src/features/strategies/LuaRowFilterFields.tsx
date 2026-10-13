import { Form, Select } from 'antd'
import { useTranslation } from 'react-i18next'
import { HintForm } from '@/components/HintForm'

export function LuaRowFilterFields({ index }: { index: number }) {
  const { t } = useTranslation()
  const form = Form.useFormInstance()
  const language = Form.useWatch(['row_filters', index, 'language'], form)
  return (
    <>
      <HintForm name={[index, 'language']} noStyle>
        <Select
          aria-label={t('strategies.editor.fields.rowFilterLanguage')}
          style={{ width: 100 }}
          placeholder="expr"
          options={['expr', 'lua'].map((value) => ({ value, label: value }))}
          onChange={(value: string) => {
            form.setFieldValue(['row_filters', index, 'columns'], undefined)
            form.setFieldValue(
              ['row_filters', index, 'expression'],
              value === 'lua' ? 'return row.age >= 18' : 'age >= 18',
            )
          }}
        />
      </HintForm>
      {language === 'lua' && (
        <HintForm name={[index, 'columns']} rules={[{ required: true }]} noStyle>
          <Select
            aria-label={t('strategies.editor.fields.rowFilterColumns')}
            placeholder={t('strategies.editor.fields.rowFilterColumns')}
            mode="tags"
            tokenSeparators={[',']}
            style={{ width: 210 }}
            maxCount={200}
          />
        </HintForm>
      )}
    </>
  )
}
