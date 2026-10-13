package pipelinespec

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/xmapst/xdts/internal/iproto"
)

// computeConfigDigest hashes a deep clone of the role-specific configuration.
// Volatile delivery metadata is excluded so retries retain the same content identity.
// The encoding/json dialect is part of the digest contract; stored rule documents
// use protojson and must not be substituted here.
func computeConfigDigest(item *iproto.PipelineConfigItem) (string, error) {
	if item == nil {
		return "", fmt.Errorf("pipelinespec: 无法为空的 PipelineConfigItem 计算 digest")
	}
	cloned, ok := proto.Clone(item).(*iproto.PipelineConfigItem)
	if !ok || cloned == nil {
		return "", fmt.Errorf("pipelinespec: proto.Clone(PipelineConfigItem) 返回了意外的类型")
	}
	cloned.ConfigVersion = 0
	if cloned.Common != nil {
		cloned.Common.ConfigVersion = 0
		cloned.Common.RunId = 0
		if cloned.Common.ReinitIdentity != nil {
			cloned.Common.ReinitIdentity.CmdSeq = 0
		}
	}

	raw, err := json.Marshal(cloned)
	if err != nil {
		return "", fmt.Errorf("pipelinespec: 序列化 PipelineConfigItem 计算 digest 失败: %w", err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}
