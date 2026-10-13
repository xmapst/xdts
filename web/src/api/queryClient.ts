// 全局 QueryClient：统一失败提示（antd message），登录页等场景可通过
// mutation/query 的 meta.suppressGlobalError 关闭全局提示，改为内联展示。
import { MutationCache, QueryCache, QueryClient } from '@tanstack/react-query'
import { hintMessage as message } from '@/components/hintFeedback'
import { ApiError } from './client'

interface SuppressibleMeta {
  suppressGlobalError?: boolean
}

function isSuppressibleMeta(value: unknown): value is SuppressibleMeta {
  return typeof value === 'object' && value !== null
}

function shouldSuppress(meta: unknown): boolean {
  return isSuppressibleMeta(meta) && meta.suppressGlobalError === true
}

function describeError(error: unknown, fallback: string): string {
  return error instanceof ApiError ? error.message : fallback
}

export const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      refetchOnWindowFocus: false,
      staleTime: 10_000,
    },
    mutations: {
      retry: 0,
    },
  },
  queryCache: new QueryCache({
    onError: (error, query) => {
      if (shouldSuppress(query.meta)) {
        return
      }
      void message.error(describeError(error, '数据加载失败'))
    },
  }),
  mutationCache: new MutationCache({
    onError: (error, _variables, _context, mutation) => {
      if (shouldSuppress(mutation.meta)) {
        return
      }
      void message.error(describeError(error, '操作失败'))
    },
  }),
})
