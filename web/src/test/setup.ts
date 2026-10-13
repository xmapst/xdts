import '@testing-library/jest-dom/vitest'
import { afterEach } from 'vitest'
import { cleanup, configure } from '@testing-library/react'

afterEach(() => {
  cleanup()
})

configure({ asyncUtilTimeout: 4000 })

// jsdom 未实现 matchMedia；antd 的响应式断点（useBreakpoint / ConfigProvider）
// 会在挂载时调用它，缺失会直接抛错导致整棵组件树渲染失败。
class MockMediaQueryList implements MediaQueryList {
  matches = false
  media: string
  onchange: ((this: MediaQueryList, ev: MediaQueryListEvent) => void) | null = null
  constructor(media: string) {
    this.media = media
  }
  addListener(): void {}
  removeListener(): void {}
  addEventListener(): void {}
  removeEventListener(): void {}
  dispatchEvent(): boolean {
    return false
  }
}

if (typeof window !== 'undefined' && typeof window.matchMedia !== 'function') {
  window.matchMedia = (query: string) => new MockMediaQueryList(query)
}

// jsdom 同样未实现 ResizeObserver；antd 的 Popconfirm / Tooltip / Popover 等基于
// rc-trigger 的组件依赖它定位弹出层，缺失会在这些组件 mount 时直接抛错
// `ResizeObserver is not defined`，导致测试失败而非渲染问题本身。
class MockResizeObserver implements ResizeObserver {
  observe(): void {}
  unobserve(): void {}
  disconnect(): void {}
}

if (typeof window !== 'undefined' && typeof window.ResizeObserver !== 'function') {
  window.ResizeObserver = MockResizeObserver
}

// Always stub object URLs: the runner implementation expects a different Blob representation than jsdom provides.
if (typeof window !== 'undefined') {
  window.URL.createObjectURL = () => 'blob:mock-url'
  window.URL.revokeObjectURL = () => {}
}
