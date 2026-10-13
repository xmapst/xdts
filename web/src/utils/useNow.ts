import { useEffect, useState } from 'react'

/**
 * 返回一个会按 `intervalMs` 周期性刷新的当前时间戳，供「距现在多久」一类的
 * 展示逻辑使用，避免直接在渲染函数体内调用 `Date.now()`（oxlint
 * react(purity) 会告警：渲染阶段调用非纯函数，结果随渲染时机不可预测）。
 */
export function useNow(intervalMs: number): number {
  const [now, setNow] = useState(() => Date.now())
  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), intervalMs)
    return () => window.clearInterval(timer)
  }, [intervalMs])
  return now
}
