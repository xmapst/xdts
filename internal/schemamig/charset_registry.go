// Register parser charset knowledge missing from TiDB's built-in registry.
// Collation IDs come from MySQL SHOW COLLATION. utf8mb3 aliases need the fidelity
// markers in charset_fidelity.go because registration alone cannot prevent alias
// rewriting. This initialization registers immutable grammar knowledge only.
package schemamig

import (
	"github.com/pingcap/tidb/pkg/parser/charset"
)

// missingCollations 是 tidb parser 不认识、但真实 MySQL 8.0.46 支持的排序
// 规则，按真实 SHOW COLLATION 的 Id 登记（不含 utf8mb3 那四个别名重定向
// 名字，见文件头说明）。
var missingCollations = []*charset.Collation{
	// utf8mb3（现代 MySQL 里 utf8 的规范名）系列，25 条——不含
	// utf8mb3_general_ci(33)/utf8mb3_bin(83)/utf8mb3_unicode_ci(192)
	// 这三个被 tidb 内部别名重定向到 utf8_* 的名字。
	{ID: 76, CharsetName: "utf8mb3", Name: "utf8mb3_tolower_ci", Sortlen: 1, PadAttribute: charset.PadNone},
	{ID: 193, CharsetName: "utf8mb3", Name: "utf8mb3_icelandic_ci", Sortlen: 1, PadAttribute: charset.PadNone},
	{ID: 194, CharsetName: "utf8mb3", Name: "utf8mb3_latvian_ci", Sortlen: 1, PadAttribute: charset.PadNone},
	{ID: 195, CharsetName: "utf8mb3", Name: "utf8mb3_romanian_ci", Sortlen: 1, PadAttribute: charset.PadNone},
	{ID: 196, CharsetName: "utf8mb3", Name: "utf8mb3_slovenian_ci", Sortlen: 1, PadAttribute: charset.PadNone},
	{ID: 197, CharsetName: "utf8mb3", Name: "utf8mb3_polish_ci", Sortlen: 1, PadAttribute: charset.PadNone},
	{ID: 198, CharsetName: "utf8mb3", Name: "utf8mb3_estonian_ci", Sortlen: 1, PadAttribute: charset.PadNone},
	{ID: 199, CharsetName: "utf8mb3", Name: "utf8mb3_spanish_ci", Sortlen: 1, PadAttribute: charset.PadNone},
	{ID: 200, CharsetName: "utf8mb3", Name: "utf8mb3_swedish_ci", Sortlen: 1, PadAttribute: charset.PadNone},
	{ID: 201, CharsetName: "utf8mb3", Name: "utf8mb3_turkish_ci", Sortlen: 1, PadAttribute: charset.PadNone},
	{ID: 202, CharsetName: "utf8mb3", Name: "utf8mb3_czech_ci", Sortlen: 1, PadAttribute: charset.PadNone},
	{ID: 203, CharsetName: "utf8mb3", Name: "utf8mb3_danish_ci", Sortlen: 1, PadAttribute: charset.PadNone},
	{ID: 204, CharsetName: "utf8mb3", Name: "utf8mb3_lithuanian_ci", Sortlen: 1, PadAttribute: charset.PadNone},
	{ID: 205, CharsetName: "utf8mb3", Name: "utf8mb3_slovak_ci", Sortlen: 1, PadAttribute: charset.PadNone},
	{ID: 206, CharsetName: "utf8mb3", Name: "utf8mb3_spanish2_ci", Sortlen: 1, PadAttribute: charset.PadNone},
	{ID: 207, CharsetName: "utf8mb3", Name: "utf8mb3_roman_ci", Sortlen: 1, PadAttribute: charset.PadNone},
	{ID: 208, CharsetName: "utf8mb3", Name: "utf8mb3_persian_ci", Sortlen: 1, PadAttribute: charset.PadNone},
	{ID: 209, CharsetName: "utf8mb3", Name: "utf8mb3_esperanto_ci", Sortlen: 1, PadAttribute: charset.PadNone},
	{ID: 210, CharsetName: "utf8mb3", Name: "utf8mb3_hungarian_ci", Sortlen: 1, PadAttribute: charset.PadNone},
	{ID: 211, CharsetName: "utf8mb3", Name: "utf8mb3_sinhala_ci", Sortlen: 1, PadAttribute: charset.PadNone},
	{ID: 212, CharsetName: "utf8mb3", Name: "utf8mb3_german2_ci", Sortlen: 1, PadAttribute: charset.PadNone},
	{ID: 213, CharsetName: "utf8mb3", Name: "utf8mb3_croatian_ci", Sortlen: 1, PadAttribute: charset.PadNone},
	{ID: 214, CharsetName: "utf8mb3", Name: "utf8mb3_unicode_520_ci", Sortlen: 1, PadAttribute: charset.PadNone},
	{ID: 215, CharsetName: "utf8mb3", Name: "utf8mb3_vietnamese_ci", Sortlen: 1, PadAttribute: charset.PadNone},
	{ID: 223, CharsetName: "utf8mb3", Name: "utf8mb3_general_mysql500_ci", Sortlen: 1, PadAttribute: charset.PadNone},

	// utf8mb4 的 0900（MySQL 8.0 UCA）本地化变体，14 条（7 种语言 ×
	// ai_ci/as_cs 两种强度），MySQL 8.0.24 前后陆续加入。
	{ID: 310, CharsetName: "utf8mb4", Name: "utf8mb4_nb_0900_ai_ci", Sortlen: 1, PadAttribute: charset.PadNone},
	{ID: 311, CharsetName: "utf8mb4", Name: "utf8mb4_nb_0900_as_cs", Sortlen: 1, PadAttribute: charset.PadNone},
	{ID: 312, CharsetName: "utf8mb4", Name: "utf8mb4_nn_0900_ai_ci", Sortlen: 1, PadAttribute: charset.PadNone},
	{ID: 313, CharsetName: "utf8mb4", Name: "utf8mb4_nn_0900_as_cs", Sortlen: 1, PadAttribute: charset.PadNone},
	{ID: 314, CharsetName: "utf8mb4", Name: "utf8mb4_sr_latn_0900_ai_ci", Sortlen: 1, PadAttribute: charset.PadNone},
	{ID: 315, CharsetName: "utf8mb4", Name: "utf8mb4_sr_latn_0900_as_cs", Sortlen: 1, PadAttribute: charset.PadNone},
	{ID: 316, CharsetName: "utf8mb4", Name: "utf8mb4_bs_0900_ai_ci", Sortlen: 1, PadAttribute: charset.PadNone},
	{ID: 317, CharsetName: "utf8mb4", Name: "utf8mb4_bs_0900_as_cs", Sortlen: 1, PadAttribute: charset.PadNone},
	{ID: 318, CharsetName: "utf8mb4", Name: "utf8mb4_bg_0900_ai_ci", Sortlen: 1, PadAttribute: charset.PadNone},
	{ID: 319, CharsetName: "utf8mb4", Name: "utf8mb4_bg_0900_as_cs", Sortlen: 1, PadAttribute: charset.PadNone},
	{ID: 320, CharsetName: "utf8mb4", Name: "utf8mb4_gl_0900_ai_ci", Sortlen: 1, PadAttribute: charset.PadNone},
	{ID: 321, CharsetName: "utf8mb4", Name: "utf8mb4_gl_0900_as_cs", Sortlen: 1, PadAttribute: charset.PadNone},
	{ID: 322, CharsetName: "utf8mb4", Name: "utf8mb4_mn_cyrl_0900_ai_ci", Sortlen: 1, PadAttribute: charset.PadNone},
	{ID: 323, CharsetName: "utf8mb4", Name: "utf8mb4_mn_cyrl_0900_as_cs", Sortlen: 1, PadAttribute: charset.PadNone},
}

func init() {
	for _, c := range missingCollations {
		charset.AddCollation(c)
	}
	registerUTF8MB3FidelityMarkers()
}
