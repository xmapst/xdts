import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import './i18n'
import App from './App.tsx'
import { authQueryKey } from '@/api/auth'
import { setUnauthorizedHandler } from '@/api/client'
import { queryClient } from '@/api/queryClient'

setUnauthorizedHandler(() => {
  queryClient.removeQueries({ queryKey: authQueryKey })
  if (window.location.pathname !== '/login') {
    window.location.assign('/login')
  }
})

const rootElement = document.getElementById('root')
if (!rootElement) {
  throw new Error('未找到 #root 挂载节点')
}

createRoot(rootElement).render(
  <StrictMode>
    <App />
  </StrictMode>,
)
