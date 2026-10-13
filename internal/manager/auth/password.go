package auth

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"golang.org/x/crypto/argon2"
)

// Argon2Params 是本地账号密码哈希使用的 argon2id 参数（需求 MGR-USR-01：
// 「密码以 argon2id 或 bcrypt 哈希存储」，本项目选 argon2id——bcrypt 有 72
// 字节密码截断且时间、内存成本耦合在一个参数里不可分离调优，argon2id 两者
// 都能独立配置，更适合按部署环境（CPU/内存资源、并发登录量）调优）。
//
// 具体取值由调用方传入（通常来自 config.Argon2Config），不在这里写死默认
// 值本身，避免出现「代码里一份默认值、配置里又一份默认值」两处不一致的
// 风险；DefaultArgon2Params 只是给测试/未显式配置场景兜底。
type Argon2Params struct {
	MemoryKiB   uint32 // 内存成本，单位 KiB
	Iterations  uint32 // 时间成本（迭代次数）
	Parallelism uint8  // 并行度（lane 数）
	SaltLength  uint32 // 盐长度，字节
	KeyLength   uint32 // 派生密钥（哈希输出）长度，字节
}

// DefaultArgon2Params 返回一组有依据的默认参数，供没有显式配置时使用
// （例如测试、或 config.Argon2Config 字段为零值的防御性兜底）。
//
// 依据 OWASP Password Storage Cheat Sheet（2023 修订版）针对 argon2id 给出
// 的推荐配置第二档：内存 19 MiB（19456 KiB）、时间成本 2 次迭代、并行度 1。
// 第一档（46 MiB、1 次迭代、并行度 1）内存占用更高，不适合与其它服务共用
// 主机部署的场景；两档对暴力破解的防护强度相当，選第二档更保守。
// 盐 16 字节、输出 32 字节是该文档同一节给出的通用建议。
func DefaultArgon2Params() Argon2Params {
	return Argon2Params{
		MemoryKiB:   19 * 1024,
		Iterations:  2,
		Parallelism: 1,
		SaltLength:  16,
		KeyLength:   32,
	}
}

// argon2idVariant 是编码字符串里标识哈希算法的字段值；本项目只产生、也只
// 接受 argon2id（不是 argon2i / argon2d），和 MGR-USR-01 的选型保持一致。
const argon2idVariant = "argon2id"

// ErrMalformedPasswordHash 表示编码后的密码哈希字符串格式不合法（例如被
// 截断、手工改过，或者不是本函数产生的编码）。正常运行时不应该出现，仅作
// 防御性处理；调用方不应该把这个错误的具体内容回显给登录请求方。
var ErrMalformedPasswordHash = errors.New("auth: 密码哈希格式不合法")

// HashPassword 用 argon2id 对明文密码做哈希，编码为自解释的字符串：
//
//	$argon2id$v=19$m=<内存KiB>,t=<迭代次数>,p=<并行度>$<盐 base64>$<哈希 base64>
//
// 参数编码进字符串本身，而不是只存哈希值：以后调高 params（例如换了更快的
// 硬件）不会让旧密码的哈希校验失败——VerifyPassword 按字符串里记录的参数
// 重新计算，而不是用调用时的「当前」配置。
func HashPassword(password string, params Argon2Params) (string, error) {
	salt := make([]byte, params.SaltLength)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("auth: 生成密码哈希盐值失败: %w", err)
	}
	hash := argon2.IDKey([]byte(password), salt, params.Iterations, params.MemoryKiB, params.Parallelism, params.KeyLength)

	encoded := fmt.Sprintf("$%s$v=%d$m=%d,t=%d,p=%d$%s$%s",
		argon2idVariant,
		argon2.Version,
		params.MemoryKiB, params.Iterations, params.Parallelism,
		base64.RawStdEncoding.EncodeToString(salt),
		base64.RawStdEncoding.EncodeToString(hash),
	)
	return encoded, nil
}

// VerifyPassword 校验明文密码是否匹配 HashPassword 产生的编码哈希。
//
// 返回值约定：
//   - (true, nil)：密码正确；
//   - (false, nil)：密码错误——这是正常的业务结果，不是异常；
//   - (false, err)：encodedHash 本身格式不合法，不应该在正常运行时出现
//     （调用方应当把这种情况和「密码错误」一样按登录失败处理，不能把 err
//     的具体内容回显给登录请求方，否则等于告诉攻击者账号数据被篡改过）。
//
// 用 subtle.ConstantTimeCompare 比较派生出的哈希，避免逐字节比较在哈希值
// 本身长度已知的前提下引入可测的时序差异。
func VerifyPassword(password, encodedHash string) (bool, error) {
	params, salt, hash, err := decodeArgon2Hash(encodedHash)
	if err != nil {
		return false, err
	}
	candidate := argon2.IDKey([]byte(password), salt, params.Iterations, params.MemoryKiB, params.Parallelism, uint32(len(hash)))
	if subtle.ConstantTimeCompare(candidate, hash) == 1 {
		return true, nil
	}
	return false, nil
}

// decodeArgon2Hash 解析 HashPassword 产生的编码字符串，取出参数、盐与哈希
// 值三部分。
func decodeArgon2Hash(encoded string) (Argon2Params, []byte, []byte, error) {
	// 形如 "$argon2id$v=19$m=..,t=..,p=..$<salt>$<hash>"，按 "$" 切分后第一
	// 个元素是空字符串（开头的 "$"）。
	parts := strings.Split(encoded, "$")
	if len(parts) != 6 || parts[0] != "" || parts[1] != argon2idVariant {
		return Argon2Params{}, nil, nil, ErrMalformedPasswordHash
	}

	var version int
	if _, err := fmt.Sscanf(parts[2], "v=%d", &version); err != nil {
		return Argon2Params{}, nil, nil, fmt.Errorf("%w: 版本字段解析失败: %v", ErrMalformedPasswordHash, err)
	}
	if version != argon2.Version {
		return Argon2Params{}, nil, nil, fmt.Errorf("%w: 不支持的 argon2 版本 %d", ErrMalformedPasswordHash, version)
	}

	var params Argon2Params
	if _, err := fmt.Sscanf(parts[3], "m=%d,t=%d,p=%d", &params.MemoryKiB, &params.Iterations, &params.Parallelism); err != nil {
		return Argon2Params{}, nil, nil, fmt.Errorf("%w: 参数字段解析失败: %v", ErrMalformedPasswordHash, err)
	}

	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil {
		return Argon2Params{}, nil, nil, fmt.Errorf("%w: 盐值解码失败: %v", ErrMalformedPasswordHash, err)
	}
	hash, err := base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil {
		return Argon2Params{}, nil, nil, fmt.Errorf("%w: 哈希值解码失败: %v", ErrMalformedPasswordHash, err)
	}
	params.SaltLength = uint32(len(salt))
	params.KeyLength = uint32(len(hash))
	return params, salt, hash, nil
}
