package http

import (
	"fmt"
	"io/fs"
	"net/http"
	"path"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/xmapst/xdts/internal/manager/static"
)

// apiPathPrefix 界定 NoRoute 分支：命中该前缀走 JSON 404，其余一律回退
// index.html 交给前端 history 路由处理（需求 MGR-WEB-01/04）。
const apiPathPrefix = "/api/"

// registerStatic 挂载内嵌的前端产物，并在 NoRoute 时按 SPA 规则回退。
func registerStatic(engine *gin.Engine) error {
	assets, err := static.FS()
	if err != nil {
		return fmt.Errorf("http: 加载内嵌前端资源失败: %w", err)
	}
	fileServer := http.FileServer(http.FS(assets))

	engine.NoRoute(func(c *gin.Context) {
		if strings.HasPrefix(c.Request.URL.Path, apiPathPrefix) {
			NotFound(c)
			return
		}

		// 命中真实静态文件（js/css/图片等）就直接交给 FileServer 处理，
		// 以便拿到正确的 Content-Type；其余一律回退 index.html，交给前端
		// history 路由自行处理（SPA 深链接刷新场景）。
		if isRealFile(assets, c.Request.URL.Path) {
			fileServer.ServeHTTP(c.Writer, c.Request)
			return
		}

		c.Request.URL.Path = "/"
		fileServer.ServeHTTP(c.Writer, c.Request)
	})
	return nil
}

// isRealFile 判断请求路径在嵌入的静态资源里是否存在对应文件。
func isRealFile(assets fs.FS, urlPath string) bool {
	cleaned := path.Clean(strings.TrimPrefix(urlPath, "/"))
	if cleaned == "." || cleaned == "" {
		return false
	}
	info, err := fs.Stat(assets, cleaned)
	if err != nil {
		return false
	}
	return !info.IsDir()
}
