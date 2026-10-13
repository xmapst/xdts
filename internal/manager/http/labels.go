package http

import (
	"encoding/json"

	"github.com/xmapst/xdts/internal/manager/model"
)

// 空/未设置一律编解码成空切片而不是 nil：nil 切片序列化成 JSON 是
// `null`，让前端每处消费的地方都要多判断一次 null，不如统一成 `[]`。

// encodeLabels 把字符串标签集合编码成 model.JSON。
func encodeLabels(labels []string) (model.JSON, error) {
	if labels == nil {
		labels = []string{}
	}
	b, err := json.Marshal(labels)
	if err != nil {
		return nil, err
	}
	return model.JSON(b), nil
}

// decodeLabels 把落库的 model.JSON 解码成字符串标签集合；NULL/空值视为
// 空切片。
func decodeLabels(raw model.JSON) ([]string, error) {
	if len(raw) == 0 {
		return []string{}, nil
	}
	var labels []string
	if err := json.Unmarshal(raw, &labels); err != nil {
		return nil, err
	}
	if labels == nil {
		labels = []string{}
	}
	return labels, nil
}
