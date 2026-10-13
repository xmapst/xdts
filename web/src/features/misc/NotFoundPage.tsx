import { Button, Result } from 'antd'
import { useTranslation } from 'react-i18next'
import { useNavigate } from 'react-router-dom'

export default function NotFoundPage() {
  const { t } = useTranslation()
  const navigate = useNavigate()
  return (
    <Result
      status="404"
      title="404"
      subTitle={t('common.unknownError')}
      extra={
        <Button type="primary" onClick={() => navigate('/dashboard')}>
          {t('nav.dashboard')}
        </Button>
      }
    />
  )
}
