#!/usr/bin/env node
// Verify that every selected test file appears in the runner report and has nonzero tests. A successful runner exit alone cannot establish coverage.

import { globSync } from 'node:fs'
import { spawnSync } from 'node:child_process'
import { readFileSync, mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve, sep } from 'node:path'

const WEB_ROOT = resolve(import.meta.dirname, '..')

/** Discover project test files while excluding dependency tests. */
export function findExpectedTestFiles(root) {
  return globSync('**/*.test.{ts,tsx,mjs,js}', {
    cwd: root,
    exclude: (relPath) => relPath.split(sep).includes('node_modules'),
  })
    .map((relPath) => resolve(root, relPath))
    .sort()
}

/** Compare normalized paths independently of the runner. */
export function computeMissingFiles(expectedAbsPaths, actualAbsPaths) {
  const normalize = (p) => resolve(p).toLowerCase()
  const actualSet = new Set(actualAbsPaths.map(normalize))
  return expectedAbsPaths.filter((expected) => !actualSet.has(normalize(expected))).sort()
}

/** Read file names and case counts; missing or malformed reports throw. */
export function parseVitestReportedFiles(jsonReportPath) {
  const raw = readFileSync(jsonReportPath, 'utf8')
  const report = JSON.parse(raw)
  const testResults = report.testResults ?? []
  const names = testResults.map((entry) => entry.name)
  const testCounts = new Map(
    testResults.map((entry) => [entry.name, (entry.assertionResults ?? []).length]),
  )
  return { names, testCounts, success: report.success === true }
}

export function filterExpectedByPositionalArgs(expectedAbsPaths, passthroughArgs) {
  const positional = passthroughArgs.filter((arg) => !arg.startsWith('-'))
  if (positional.length === 0) {
    return expectedAbsPaths
  }
  const patterns = positional.map((p) => p.toLowerCase())
  return expectedAbsPaths.filter((absPath) => {
    const normalized = absPath.toLowerCase()
    return patterns.some((pattern) => normalized.includes(pattern))
  })
}

/** Report files present in the runner report with zero recorded cases. */
export function computeZeroTestFiles(expectedAbsPaths, testCounts) {
  const normalize = (p) => resolve(p).toLowerCase()
  const countByNormalized = new Map(
    Array.from(testCounts.entries()).map(([name, count]) => [normalize(name), count]),
  )
  return expectedAbsPaths
    .filter((expected) => countByNormalized.get(normalize(expected)) === 0)
    .sort()
}

function main() {
  const passthroughArgs = process.argv.slice(2)
  const expected = findExpectedTestFiles(WEB_ROOT)
  // Targeted runs must still report every selected file with nonzero cases.
  const targetedExpected = filterExpectedByPositionalArgs(expected, passthroughArgs)
  const isTargetedRun = targetedExpected.length !== expected.length

  const tmpDir = mkdtempSync(join(tmpdir(), 'xdts-vitest-coverage-'))
  const jsonReportPath = join(tmpDir, 'report.json')

  const result = spawnSync(
    'npx',
    [
      'vitest',
      'run',
      '--reporter=default',
      '--reporter=json',
      `--outputFile=${jsonReportPath}`,
      ...passthroughArgs,
    ],
    { cwd: WEB_ROOT, stdio: 'inherit', shell: process.platform === 'win32' },
  )
  const vitestExitCode = result.status ?? 1

  let missing = []
  let zeroTest = []
  let parseFailed = false
  try {
    const { names, testCounts } = parseVitestReportedFiles(jsonReportPath)
    missing = computeMissingFiles(targetedExpected, names)
    zeroTest = computeZeroTestFiles(targetedExpected, testCounts)
  } catch (err) {
    parseFailed = true
    console.error(
      `\n[verifyTestCoverage] 无法解析 vitest 的 JSON 结果（${jsonReportPath}）：${err.message}\n` +
        '  这比"某个文件被跳过"更严重（vitest 可能在写出报告前就整体崩溃），' +
        '按验证失败处理，不当成零个文件缺失。\n',
    )
  } finally {
    rmSync(tmpDir, { recursive: true, force: true })
  }

  if (missing.length > 0) {
    console.error(
      `\n[verifyTestCoverage] ${isTargetedRun ? '本次定向跑理应命中的文件里，有' : '磁盘上有'} ${missing.length} 个测试文件从未出现在 vitest 的执行结果里` +
        '（很可能是 vitest 5.0.1 的 fork-pool worker 超时问题，被静默跳过，' +
        '即使上面 vitest 自己的汇总显示"全部通过"也不能采信）：\n' +
        missing.map((f) => `  - ${f.replace(WEB_ROOT + sep, '')}`).join('\n') +
        '\n',
    )
  }

  if (zeroTest.length > 0) {
    console.error(
      `\n[verifyTestCoverage] 有 ${zeroTest.length} 个测试文件出现在 vitest 结果里，但一条用例都没记录到` +
        '（可能是文件本身有语法错误导致 describe/it 全部没能注册，或者 worker 在真正' +
        '执行用例之前就中断——同样不能采信"全部通过"）：\n' +
        zeroTest.map((f) => `  - ${f.replace(WEB_ROOT + sep, '')}`).join('\n') +
        '\n',
    )
  }

  if (vitestExitCode !== 0) {
    console.error(`\n[verifyTestCoverage] vitest 本身以非零退出码 ${vitestExitCode} 结束。`)
    process.exit(vitestExitCode)
  }
  if (parseFailed) {
    process.exit(1)
  }
  if (missing.length > 0 || zeroTest.length > 0) {
    process.exit(1)
  }

  if (!isTargetedRun) {
    console.log(
      `\n[verifyTestCoverage] 磁盘 ${expected.length} 个测试文件全部在 vitest 结果中出现，未发现被静默跳过的文件。`,
    )
  } else if (targetedExpected.length === 0) {
    console.log(
      '\n[verifyTestCoverage] 本次传入的参数未能匹配到磁盘上任何已知测试文件（按文件路径子串比对），' +
        '跳过"结果里确实出现且用例数非零"的针对性核对，仅依赖 vitest 自身的退出码。',
    )
  } else {
    console.log(
      `\n[verifyTestCoverage] 定向跑理应命中的 ${targetedExpected.length} 个测试文件全部在 vitest 结果中出现且至少有一条用例，未发现被静默跳过的文件。`,
    )
  }
  process.exit(0)
}

// Imports expose pure helpers without starting the runner.
if (import.meta.url === `file://${process.argv[1]}`) {
  main()
}
