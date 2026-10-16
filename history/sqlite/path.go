package sqlite

import (
	"net/url"
	"path/filepath"
	"strings"
)

func databaseURL(path string) *url.URL {
	urlPath := filepath.ToSlash(path)
	// Windows 盘符必须放在 URI 路径内，否则 net/url 会生成 file://C:…，
	// SQLite 会把盘符当作不受支持的 authority，导致数据库连接失败。
	if !strings.HasPrefix(urlPath, "/") {
		urlPath = "/" + urlPath
	}
	return &url.URL{Scheme: "file", Path: urlPath}
}
