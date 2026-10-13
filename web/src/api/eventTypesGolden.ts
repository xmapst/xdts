import eventTypesGolden from '../../../internal/manager/model/event_types.golden.json'

/* 金样里的取值。静态类型是 `string[]` 而不是字面量联合——实测确认
`resolveJsonModule` 导入的 JSON 数组推不出字面量
（`const probe: (typeof golden.event_types)[number] = 'not_a_type'` 编译通过）。
所以字面量 union 只能被机械校验、不能由它生成，见 `web/src/api/events.ts`。 */
export const GOLDEN_EVENT_TYPES: readonly string[] = eventTypesGolden.event_types
