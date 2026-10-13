import { describe, expect, it } from 'vitest'
import {
  computeMissingFiles,
  computeZeroTestFiles,
  filterExpectedByPositionalArgs,
  findExpectedTestFiles,
  parseVitestReportedFiles,
} from './verifyTestCoverage.mjs'
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'

describe('computeMissingFiles', () => {
  it('detects a file that exists on disk but never appears in vitest results (positive control)', () => {
    const expected = [
      '/repo/web/src/a.test.ts',
      '/repo/web/src/b.test.ts',
      '/repo/web/src/c.test.ts',
    ]
    // 模拟 vitest 的 fork-pool worker 超时：b.test.ts 被整个跳过，从未出现在
    // 它自己上报的 JSON 结果里，但 a/c 两个文件都正常跑完了。
    const actual = ['/repo/web/src/a.test.ts', '/repo/web/src/c.test.ts']

    const missing = computeMissingFiles(expected, actual)

    expect(missing).toEqual(['/repo/web/src/b.test.ts'])
  })

  it('reports nothing missing when every expected file appears in the results', () => {
    const expected = ['/repo/web/src/a.test.ts', '/repo/web/src/b.test.ts']
    const actual = ['/repo/web/src/b.test.ts', '/repo/web/src/a.test.ts']

    expect(computeMissingFiles(expected, actual)).toEqual([])
  })

  it('detects multiple simultaneously-dropped files, not just the first one', () => {
    const expected = [
      '/repo/web/src/a.test.ts',
      '/repo/web/src/b.test.ts',
      '/repo/web/src/c.test.ts',
    ]
    const actual = ['/repo/web/src/b.test.ts']

    expect(computeMissingFiles(expected, actual)).toEqual([
      '/repo/web/src/a.test.ts',
      '/repo/web/src/c.test.ts',
    ])
  })

  it('is tolerant of path casing/separator differences that are not real drops', () => {
    const expected = ['/repo/web/src/A.test.ts']
    const actual = ['/repo/web/src/a.test.ts']

    expect(computeMissingFiles(expected, actual)).toEqual([])
  })
})

describe('findExpectedTestFiles', () => {
  it('finds this very test file among the real on-disk test files', () => {
    // 不 mock 文件系统，直接对这个仓库的真实 web/ 目录跑一次——用来确认
    // glob 模式本身是对的（能找到测试文件、不会因为拼错扩展名或者漏掉
    // exclude 而找出 node_modules 里的东西）。
    const webRoot = resolve(import.meta.dirname, '..')
    const files = findExpectedTestFiles(webRoot)

    expect(files.some((f) => f.endsWith('verifyTestCoverage.test.mjs'))).toBe(true)
    expect(files.every((f) => !f.includes('node_modules'))).toBe(true)
  })
})

describe('filterExpectedByPositionalArgs', () => {
  const expected = [
    '/repo/web/src/features/users/UsersPage.test.tsx',
    '/repo/web/src/features/nodes/NodesPage.test.tsx',
    '/repo/web/src/utils/exportCsv.test.ts',
  ]

  it('narrows to the subset matching a single positional file path (the common `npm run test -- <file>` case)', () => {
    const targeted = filterExpectedByPositionalArgs(expected, [
      'src/features/users/UsersPage.test.tsx',
    ])

    expect(targeted).toEqual(['/repo/web/src/features/users/UsersPage.test.tsx'])
  })

  it('narrows to a directory-level subset when the positional arg is a folder path', () => {
    const targeted = filterExpectedByPositionalArgs(expected, ['src/features/users'])

    expect(targeted).toEqual(['/repo/web/src/features/users/UsersPage.test.tsx'])
  })

  it('unions multiple positional args instead of only matching the first one', () => {
    const targeted = filterExpectedByPositionalArgs(expected, [
      'UsersPage.test.tsx',
      'exportCsv.test.ts',
    ])

    expect(targeted.sort()).toEqual(
      [
        '/repo/web/src/features/users/UsersPage.test.tsx',
        '/repo/web/src/utils/exportCsv.test.ts',
      ].sort(),
    )
  })

  it('matches case-insensitively', () => {
    const targeted = filterExpectedByPositionalArgs(expected, ['userspage.test.tsx'])

    expect(targeted).toEqual(['/repo/web/src/features/users/UsersPage.test.tsx'])
  })

  it('ignores flag-style args (leading "-") and does not treat them as file filters', () => {
    const targeted = filterExpectedByPositionalArgs(expected, ['--coverage', '--reporter=verbose'])

    // 一个位置参数都没有：vitest 的实际行为是"跑全部"，不是"跑空集合"——
    // 必须返回完整集合，否则会把一次事实上的全量跑误判成"目标是空集合"。
    expect(targeted).toEqual(expected)
  })

  it('returns the full set unchanged when no args are given at all', () => {
    expect(filterExpectedByPositionalArgs(expected, [])).toEqual(expected)
  })

  it('returns an empty subset when the positional arg matches nothing on disk (e.g. a typo)', () => {
    const targeted = filterExpectedByPositionalArgs(expected, ['src/does/not/exist.test.ts'])

    expect(targeted).toEqual([])
  })
})

describe('computeZeroTestFiles', () => {
  it('detects a file that appears in results but recorded zero test cases (positive control)', () => {
    const expected = ['/repo/web/src/a.test.ts', '/repo/web/src/b.test.ts']
    // a 正常跑了 3 条用例；b 出现在结果里（worker 起来了），但一条用例都没
    // 记录到——比如文件语法错误导致 describe/it 全部没能注册。
    const testCounts = new Map([
      ['/repo/web/src/a.test.ts', 3],
      ['/repo/web/src/b.test.ts', 0],
    ])

    expect(computeZeroTestFiles(expected, testCounts)).toEqual(['/repo/web/src/b.test.ts'])
  })

  it("does not flag a file that is simply absent from testCounts (that is computeMissingFiles' job)", () => {
    const expected = ['/repo/web/src/a.test.ts', '/repo/web/src/b.test.ts']
    // b 完全没出现在 vitest 结果里——这是"被静默跳过"，不是"零用例"，
    // 两个函数的职责边界必须清楚，不能被同一个文件同时点两次名。
    const testCounts = new Map([['/repo/web/src/a.test.ts', 3]])

    expect(computeZeroTestFiles(expected, testCounts)).toEqual([])
  })

  it('reports nothing when every appearing file has at least one test case', () => {
    const expected = ['/repo/web/src/a.test.ts', '/repo/web/src/b.test.ts']
    const testCounts = new Map([
      ['/repo/web/src/a.test.ts', 3],
      ['/repo/web/src/b.test.ts', 1],
    ])

    expect(computeZeroTestFiles(expected, testCounts)).toEqual([])
  })

  it('detects multiple simultaneous zero-test files, not just the first one', () => {
    const expected = [
      '/repo/web/src/a.test.ts',
      '/repo/web/src/b.test.ts',
      '/repo/web/src/c.test.ts',
    ]
    const testCounts = new Map([
      ['/repo/web/src/a.test.ts', 0],
      ['/repo/web/src/b.test.ts', 2],
      ['/repo/web/src/c.test.ts', 0],
    ])

    expect(computeZeroTestFiles(expected, testCounts)).toEqual([
      '/repo/web/src/a.test.ts',
      '/repo/web/src/c.test.ts',
    ])
  })

  it('is tolerant of path casing differences that are not real drops', () => {
    const expected = ['/repo/web/src/A.test.ts']
    const testCounts = new Map([['/repo/web/src/a.test.ts', 5]])

    expect(computeZeroTestFiles(expected, testCounts)).toEqual([])
  })
})

describe('parseVitestReportedFiles', () => {
  it('extracts per-file test counts (assertionResults.length) alongside file names, against a realistic report shape', () => {
    const tmpDir = mkdtempSync(join(tmpdir(), 'verify-test-coverage-parse-'))
    const reportPath = join(tmpDir, 'report.json')
    try {
      writeFileSync(
        reportPath,
        JSON.stringify({
          success: true,
          testResults: [
            {
              name: '/repo/web/src/utils/exportCsv.test.ts',
              status: 'passed',
              assertionResults: [
                { status: 'passed', title: 'a' },
                { status: 'passed', title: 'b' },
              ],
            },
            {
              name: '/repo/web/src/broken.test.ts',
              status: 'passed',
              assertionResults: [],
            },
          ],
        }),
      )

      const { names, testCounts, success } = parseVitestReportedFiles(reportPath)

      expect(names).toEqual([
        '/repo/web/src/utils/exportCsv.test.ts',
        '/repo/web/src/broken.test.ts',
      ])
      expect(testCounts.get('/repo/web/src/utils/exportCsv.test.ts')).toBe(2)
      expect(testCounts.get('/repo/web/src/broken.test.ts')).toBe(0)
      expect(success).toBe(true)
    } finally {
      rmSync(tmpDir, { recursive: true, force: true })
    }
  })
})

describe('targeted-run protection (filterExpectedByPositionalArgs + computeMissingFiles + computeZeroTestFiles together)', () => {
  const diskFiles = [
    '/repo/web/src/features/users/UsersPage.test.tsx',
    '/repo/web/src/features/nodes/NodesPage.test.tsx',
  ]
  const passthroughArgs = ['src/features/users/UsersPage.test.tsx']

  it('flags the specifically-requested file as missing when it silently drops out of a targeted run', () => {
    const targeted = filterExpectedByPositionalArgs(diskFiles, passthroughArgs)

    // 模拟这个被点名的文件 worker 没起来，vitest 结果里完全没有它。
    const actualNames = []

    expect(computeMissingFiles(targeted, actualNames)).toEqual([
      '/repo/web/src/features/users/UsersPage.test.tsx',
    ])
  })

  it('flags the specifically-requested file as a zero-test drop when it appears but records no cases', () => {
    const targeted = filterExpectedByPositionalArgs(diskFiles, passthroughArgs)
    const testCounts = new Map([['/repo/web/src/features/users/UsersPage.test.tsx', 0]])

    expect(computeZeroTestFiles(targeted, testCounts)).toEqual([
      '/repo/web/src/features/users/UsersPage.test.tsx',
    ])
  })

  it('does NOT flag an unrelated on-disk file that this targeted run never intended to touch (preserves existing correct behavior)', () => {
    const targeted = filterExpectedByPositionalArgs(diskFiles, passthroughArgs)
    // NodesPage.test.tsx 从未出现在结果里是正常的——这次调用压根没打算跑它，
    // 不能被误判成"漏跑"。
    const actualNames = ['/repo/web/src/features/users/UsersPage.test.tsx']

    expect(computeMissingFiles(targeted, actualNames)).toEqual([])
  })

  it('passes cleanly when the requested file appears with a nonzero test count', () => {
    const targeted = filterExpectedByPositionalArgs(diskFiles, passthroughArgs)
    const actualNames = ['/repo/web/src/features/users/UsersPage.test.tsx']
    const testCounts = new Map([['/repo/web/src/features/users/UsersPage.test.tsx', 16]])

    expect(computeMissingFiles(targeted, actualNames)).toEqual([])
    expect(computeZeroTestFiles(targeted, testCounts)).toEqual([])
  })
})
