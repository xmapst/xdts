package iproto

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/proto"
)

var ErrBodyTooLarge = errors.New("iproto: decoded message exceeds byte limit")

// DecodeLimited bounds the decompressed wire payload before protobuf allocation.
// It preserves Decode's encoding/error contracts and leaves msg untouched when
// the payload exceeds maxBytes. Zero is a strict zero-byte limit.
func DecodeLimited(body []byte, encoding string, msg proto.Message, maxBytes uint64) error {
	raw := body
	switch encoding {
	case ContentEncodingIdentity:
		if uint64(len(raw)) > maxBytes {
			return ErrBodyTooLarge
		}
	case ContentEncodingZstd:
		// Streaming bounds output even when a frame omits its content size.
		// The decoder's window is separately bounded; tiny valid messages can
		// still use the encoder's default window.
		if maxBytes >= uint64(1<<63-1) {
			return errors.New("iproto: decoded byte limit exceeds supported range")
		}
		if size, single := singleFrameContentSize(body); single && size <= maxBytes {
			dec, release := borrowBoundedDecoder(body, size)
			if dec == nil {
				var err error
				dec, err = zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecodeAllCapLimit(true),
					zstd.WithDecoderMaxMemory(max(maxBytes, 8<<20)), zstd.WithDecoderMaxWindow(max(maxBytes, 8<<20)))
				if err != nil {
					return fmt.Errorf("%w: %w", ErrBodyDecompressFailed, ErrMalformedMessage)
				}
				release = func(bool) { dec.Close() }
			}
			// DecodeAll releases all input references before the exclusive decoder
			// returns to this bounded cache. Never retain a failed decoder.
			var decodeErr error
			raw, decodeErr = decodeBoundedFrame(dec, release, body, size)
			if decodeErr != nil {
				return fmt.Errorf("%w: %w: %w", ErrBodyDecompressFailed, decodeErr, ErrMalformedMessage)
			}
			break
		}
		dec, err := zstd.NewReader(bytes.NewReader(body), zstd.WithDecoderConcurrency(1),
			zstd.WithDecoderMaxMemory(max(maxBytes, 8<<20)), zstd.WithDecoderMaxWindow(max(maxBytes, 8<<20)))
		if err != nil {
			return fmt.Errorf("%w: %w", ErrBodyDecompressFailed, ErrMalformedMessage)
		}
		defer dec.Close()
		raw, err = io.ReadAll(io.LimitReader(dec, int64(maxBytes)+1))
		if err != nil {
			return fmt.Errorf("%w: %w: %w", ErrBodyDecompressFailed, err, ErrMalformedMessage)
		}
		if uint64(len(raw)) > maxBytes {
			return ErrBodyTooLarge
		}
	default:
		return fmt.Errorf("%w: %s: %w", ErrUnsupportedContentEncoding, encoding, ErrMalformedMessage)
	}
	if err := proto.Unmarshal(raw, msg); err != nil {
		return fmt.Errorf("%w: %w: %w", ErrBodyUnmarshalFailed, err, ErrMalformedMessage)
	}
	return nil
}

// EncodeOptions 决定是否压缩及 Properties.ContentEncoding。数据面使用 zstd，
// 控制面可使用 identity；编码选择由调用方负责。
type EncodeOptions struct {
	Compress bool
}

// Encode 把 protobuf 消息序列化为 wire bytes，并返回应写入 Properties.ContentEncoding
// 的值，供 NewEnvelope 使用。
func Encode(msg proto.Message, opt EncodeOptions) (body []byte, contentEncoding string, err error) {
	raw, err := proto.Marshal(msg)
	if err != nil {
		return nil, "", fmt.Errorf("iproto: protobuf 序列化失败: %w", err)
	}
	if !opt.Compress {
		return raw, ContentEncodingIdentity, nil
	}
	compressed, err := zstdEncode(raw)
	if err != nil {
		return nil, "", fmt.Errorf("iproto: zstd 压缩失败: %w", err)
	}
	return compressed, ContentEncodingZstd, nil
}

// Decode 按 contentEncoding 解压后反序列化到 msg（msg 必须是指向具体消息类型的指针，
// 由调用方按 Properties.Type 先行判定好消息类型，本包不做类型分派——调用方按
// Properties.Type 选错目标类型时，proto3 的 wire-format 解码完全不保证报错，见
// ErrBodyUnmarshalFailed 文档"实测：类型错位完全不保证报错"一段的真实复现记录）。
func Decode(body []byte, contentEncoding string, msg proto.Message) error {
	raw := body
	switch contentEncoding {
	case ContentEncodingIdentity:
		// 不压缩，raw 即 body。
	case ContentEncodingZstd:
		var err error
		raw, err = zstdDecode(body)
		if err != nil {
			return fmt.Errorf("%w: %w: %w", ErrBodyDecompressFailed, err, ErrMalformedMessage)
		}
	default:
		return fmt.Errorf("%w: %s: %w", ErrUnsupportedContentEncoding, contentEncoding, ErrMalformedMessage)
	}
	if err := proto.Unmarshal(raw, msg); err != nil {
		return fmt.Errorf("%w: %w: %w", ErrBodyUnmarshalFailed, err, ErrMalformedMessage)
	}
	return nil
}

// Decode failures match both their specific sentinel and ErrMalformedMessage.
// Protobuf wire compatibility does not establish the expected message type; callers
// must validate the envelope's type before decoding.
var (
	ErrUnsupportedContentEncoding = errors.New("iproto: 不支持的 content_encoding")
	ErrBodyDecompressFailed       = errors.New("iproto: 消息体解压失败")
	ErrBodyUnmarshalFailed        = errors.New("iproto: protobuf 反序列化失败")
)

var (
	zstdEncoderOnce sync.Once
	zstdEncoder     *zstd.Encoder
	zstdEncoderErr  error

	zstdDecoderOnce sync.Once
	zstdDecoder     *zstd.Decoder
	zstdDecoderErr  error
)

func getZstdEncoder() (*zstd.Encoder, error) {
	zstdEncoderOnce.Do(func() {
		zstdEncoder, zstdEncoderErr = zstd.NewWriter(nil)
	})
	return zstdEncoder, zstdEncoderErr
}

func getZstdDecoder() (*zstd.Decoder, error) {
	zstdDecoderOnce.Do(func() {
		zstdDecoder, zstdDecoderErr = zstd.NewReader(nil)
	})
	return zstdDecoder, zstdDecoderErr
}

func zstdEncode(raw []byte) ([]byte, error) {
	enc, err := getZstdEncoder()
	if err != nil {
		return nil, err
	}
	return enc.EncodeAll(raw, make([]byte, 0, len(raw))), nil
}

func zstdDecode(compressed []byte) ([]byte, error) {
	dec, err := getZstdDecoder()
	if err != nil {
		return nil, err
	}
	return dec.DecodeAll(compressed, nil)
}
