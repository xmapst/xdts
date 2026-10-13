// Package static 承载 Manager 内嵌的前端构建产物（`vite build`
// 输出到 Manager 包内 embed 目录，通过 //go:embed 打入二进制）。
//
// dist/ 由 web/ 的 `vite build` 产出，web/vite.config.ts 的 build.outDir 需要
// 指向本目录。
//
// 当前实际内容：ls internal/manager/static/dist/
package static

import (
	"embed"
	"io/fs"
)

//go:embed dist
var distFS embed.FS

// DistDir 是 embed 文件系统里静态资源的子目录名。
const DistDir = "dist"

// FS 返回剥离了 "dist/" 前缀的静态资源文件系统，可直接作为
// http.FileServer(http.FS(...)) 的根使用。
func FS() (fs.FS, error) {
	return fs.Sub(distFS, DistDir)
}
