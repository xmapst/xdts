import { readFile, writeFile } from 'node:fs/promises'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const sourcePath = fileURLToPath(import.meta.url)
const root = resolve(dirname(sourcePath), '..')
const identifier = /^[A-Za-z_][A-Za-z0-9_]*$/
const reserved = new Set([
  'class',
  'function',
  'const',
  'let',
  'var',
  'default',
  'return',
  'new',
  'delete',
  'input',
  'query',
  'url',
  'params',
])
function name(value) {
  if (!identifier.test(value) || reserved.has(value))
    throw new Error(`Unsupported identifier: ${value}`)
  return value
}

// A deliberately bounded OpenAPI 3.0 generator. Unsupported schema constructs
// fail explicitly rather than quietly weakening the generated contract.
export function generateCode(spec) {
  if (!/^3\.0\./.test(spec.openapi)) throw new Error('Expected OpenAPI 3.0')
  const schemas = spec.components?.schemas ?? {}
  const resolveRef = (ref) => {
    const prefix = '#/components/schemas/'
    if (!ref.startsWith(prefix) || !Object.hasOwn(schemas, ref.slice(prefix.length))) {
      throw new Error(`Unknown schema reference: ${ref}`)
    }
    return name(ref.slice(prefix.length))
  }
  const scalarParameter = (schema) => {
    const visited = new Set()
    while (schema?.$ref) {
      const key = resolveRef(schema.$ref)
      if (visited.has(key)) throw new Error(`Circular parameter schema: ${key}`)
      visited.add(key)
      schema = schemas[key]
    }
    return !schema?.nullable && ['string', 'integer', 'number', 'boolean'].includes(schema?.type)
  }
  function type(schema) {
    if (!schema || typeof schema !== 'object') throw new Error('Missing schema')
    // An explicit unconstrained schema represents the shared error-details
    // boundary. Domain fields continue to require their actual typed schema.
    if (Object.keys(schema).length === 0) return 'unknown'
    const supported = new Set([
      '$ref',
      'type',
      'enum',
      'nullable',
      'properties',
      'required',
      'items',
      'additionalProperties',
      'description',
      'format',
      'example',
      'default',
      'minLength',
      'maxLength',
      'minimum',
      'maximum',
      'minItems',
      'maxItems',
      'readOnly',
      'writeOnly',
      'oneOf',
    ])
    for (const key of Object.keys(schema)) {
      if (!supported.has(key)) throw new Error(`Unsupported schema keyword: ${key}`)
    }
    let result
    if (schema.oneOf) {
      if (
        schema.$ref ||
        schema.type ||
        schema.enum ||
        !Array.isArray(schema.oneOf) ||
        schema.oneOf.length < 2 ||
        schema.oneOf.length > 4
      )
        throw new Error('Unsupported oneOf schema')
      const branches = new Set()
      for (const branch of schema.oneOf) {
        if (
          !['string', 'integer', 'number', 'boolean'].includes(branch.type) ||
          branch.nullable ||
          branch.$ref ||
          branch.oneOf ||
          branches.has(branch.type)
        )
          throw new Error('oneOf supports distinct non-null scalar branches only')
        branches.add(branch.type)
      }
      result = `(${schema.oneOf.map(type).join(' | ')})`
    } else if (schema.$ref) {
      result = `Generated${resolveRef(schema.$ref)}`
    } else if (schema.enum) {
      if (
        !Array.isArray(schema.enum) ||
        !schema.enum.length ||
        schema.enum.some((v) => !['string', 'number', 'boolean'].includes(typeof v))
      )
        throw new Error('Invalid enum')
      result = schema.enum.map((v) => JSON.stringify(v)).join(' | ')
    } else {
      switch (schema.type) {
        case 'string':
          result = 'string'
          break
        case 'number':
        case 'integer':
          result = 'number'
          break
        case 'boolean':
          result = 'boolean'
          break
        case 'array':
          result = `Array<${type(schema.items)}>`
          break
        case 'object': {
          const props = schema.properties ?? {}
          for (const key of schema.required ?? []) {
            if (!Object.hasOwn(props, key)) throw new Error(`Required property missing: ${key}`)
          }
          const members = Object.entries(props).map(
            ([key, value]) =>
              `${JSON.stringify(key)}${schema.required?.includes(key) ? '' : '?'}: ${type(value)}`,
          )
          if (schema.additionalProperties && Object.keys(props).length)
            throw new Error('Mixed object properties and additionalProperties are unsupported')
          if (schema.additionalProperties)
            result = `Record<string, ${schema.additionalProperties === true ? 'unknown' : type(schema.additionalProperties)}>`
          else result = members.length ? `{ ${members.join('; ')} }` : 'Record<string, unknown>'
          break
        }
        default:
          throw new Error(`Unsupported schema type: ${schema.type}`)
      }
    }
    return schema.nullable ? `(${result}) | null` : result
  }
  const output = [
    '// Code generated by scripts/generate-api.mjs from openapi/xdts.yaml; DO NOT EDIT.',
    '',
    "import { apiClient, apiRequest, type ApiRequestOptions } from './client'",
    '',
  ]
  for (const [key, schema] of Object.entries(schemas))
    output.push(`export type Generated${name(key)} = ${type(schema)}`, '')
  const operations = new Set()
  for (const [path, item] of Object.entries(spec.paths ?? {})) {
    for (const [method, op] of Object.entries(item)) {
      if (method === 'parameters' || method === 'description' || method === 'summary') continue
      if (!['get', 'post', 'put', 'patch', 'delete'].includes(method))
        throw new Error(`Unsupported method: ${method}`)
      const opName = name(op.operationId)
      if (operations.has(opName)) throw new Error(`Duplicate operationId: ${opName}`)
      operations.add(opName)
      const parameters = [...(item.parameters ?? []), ...(op.parameters ?? [])]
      const args = []
      const pathParams = new Map()
      const queryParams = []
      for (const param of parameters) {
        name(param.name)
        if (param.in === 'path') {
          if (!param.required || pathParams.has(param.name))
            throw new Error('Path parameters must be unique and required')
          pathParams.set(param.name, param)
          args.push(`${param.name}: ${type(param.schema)}`)
        } else if (param.in === 'query' && scalarParameter(param.schema)) queryParams.push(param)
        else throw new Error(`Unsupported parameter: ${param.in}`)
      }
      const placeholders = [...path.matchAll(/\{([^}]+)\}/g)].map((m) => m[1])
      if (
        placeholders.some((p) => !pathParams.has(p)) ||
        [...pathParams.keys()].some((p) => !placeholders.includes(p))
      )
        throw new Error(`Path parameters do not match ${path}`)
      const segments = path
        .split(/\{([^}]+)\}/g)
        .map((part, i) =>
          i % 2 ? `encodeURIComponent(String(${name(part)}))` : JSON.stringify(part),
        )
      let body
      if (op.requestBody) {
        if (method === 'get') throw new Error(`Request body unsupported for ${method}`)
        if (!op.requestBody.required) throw new Error('Optional request bodies are unsupported')
        body = op.requestBody.content?.['application/json']?.schema
        args.push(`input: ${type(body)}`)
      }
      if (queryParams.length)
        args.push(
          `query${queryParams.some((p) => p.required) ? '' : '?'}: { ${queryParams.map((p) => `${JSON.stringify(p.name)}${p.required ? '' : '?'}: ${type(p.schema)}`).join('; ')} }`,
        )
      args.push("options?: Omit<ApiRequestOptions, 'method' | 'body'>")
      const success = Object.entries(op.responses ?? {}).filter(([status]) =>
        /^2\d\d$/.test(status),
      )
      if (success.length !== 1) throw new Error(`Expected one success response: ${opName}`)
      if (op['x-xdts-download']) {
        const content = success[0][1].content
        const media = Object.keys(content ?? {})
        if (
          method !== 'get' ||
          body ||
          queryParams.length ||
          success[0][0] !== '200' ||
          media.length !== 1 ||
          media[0] !== 'application/sql' ||
          type(content[media[0]].schema) !== 'string'
        )
          throw new Error(`Unsupported download: ${opName}`)
        output.push(
          `export function ${opName}(${args.slice(0, -1).join(', ')}): string {`,
          `  return '/api/v1' + ${segments.join(' + ')}`,
          '}',
          '',
        )
        continue
      }
      let responseType = 'void'
      if (success[0][0] !== '204') {
        const response = success[0][1].content?.['application/json']?.schema
        type(response) // Validate the full transport schema, including references.
        if (op['x-xdts-envelope']) {
          const envelope = response.$ref ? schemas[resolveRef(response.$ref)] : response
          if (!envelope.required?.includes('data') || !envelope.properties?.data)
            throw new Error('Response envelope requires data')
          responseType = type(envelope.properties.data)
        } else responseType = type(response)
      }
      output.push(
        `export function ${opName}(${args.join(', ')}): Promise<${responseType}> {`,
        `  let url = ${segments.join(' + ')}`,
      )
      if (queryParams.length) {
        output.push('  const params = new URLSearchParams()')
        for (const p of queryParams)
          output.push(
            `  if (query?.${p.name} !== undefined) params.set(${JSON.stringify(p.name)}, String(query.${p.name}))`,
          )
        output.push("  if (params.size) url += '?' + params.toString()")
      }
      // const avoids a needless mutable local when no query string is appended.
      if (!queryParams.length)
        output[output.length - 1] = output[output.length - 1].replace('let url', 'const url')
      if (method === 'delete' && body)
        output.push(
          `  return apiRequest<${responseType}>(url, { ...options, method: 'DELETE', body: input })`,
          '}',
          '',
        )
      else
        output.push(
          `  return apiClient.${method}<${responseType}>(url${['post', 'put', 'patch'].includes(method) ? (body ? ', input' : ', undefined') : ''}, options)`,
          '}',
          '',
        )
    }
  }
  return output.join('\n')
}

if (process.argv[1] && resolve(process.argv[1]) === sourcePath) {
  const option = (key, fallback) => {
    const index = process.argv.indexOf(key)
    if (index < 0) return fallback
    if (!process.argv[index + 1] || process.argv[index + 1].startsWith('--'))
      throw new Error(`Missing value for ${key}`)
    return resolve(process.argv[index + 1])
  }
  const spec = JSON.parse(
    await readFile(option('--spec', resolve(root, 'openapi/xdts.yaml')), 'utf8'),
  )
  const output = generateCode(spec)
  const destination = option('--output', resolve(root, 'src/api/generated.ts'))
  if (process.argv.includes('--check')) {
    if ((await readFile(destination, 'utf8')) !== output)
      throw new Error('Generated API drift: run npm run generate:api')
  } else await writeFile(destination, output)
}
