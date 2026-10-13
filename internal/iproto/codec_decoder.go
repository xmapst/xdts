package iproto

import (
	"sync"

	"github.com/klauspost/compress/zstd"
)

const reusableDecoderLimit = 8 << 20

// This cache holds codec machinery only. Tickets bound retained instances and
// borrow is nonblocking; each admitted caller owns one decoder until release.
var boundedDecoders struct {
	once    sync.Once
	tickets chan struct{}
	idle    chan *zstd.Decoder
}

func borrowBoundedDecoder(body []byte, size uint64) (*zstd.Decoder, func(bool)) {
	if size > reusableDecoderLimit {
		return nil, nil
	}
	var header zstd.Header
	if header.Decode(body) != nil || !header.SingleSegment && header.WindowSize > reusableDecoderLimit {
		return nil, nil
	}
	boundedDecoders.once.Do(func() {
		boundedDecoders.tickets = make(chan struct{}, 8)
		boundedDecoders.idle = make(chan *zstd.Decoder, 8)
	})
	select {
	case boundedDecoders.tickets <- struct{}{}:
	default:
		return nil, nil
	}
	var decoder *zstd.Decoder
	select {
	case decoder = <-boundedDecoders.idle:
	default:
		var err error
		decoder, err = zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecodeAllCapLimit(true), zstd.WithDecoderMaxMemory(reusableDecoderLimit), zstd.WithDecoderMaxWindow(reusableDecoderLimit))
		if err != nil {
			<-boundedDecoders.tickets
			return nil, nil
		}
	}
	return decoder, func(reuse bool) {
		if reuse {
			boundedDecoders.idle <- decoder
		} else {
			decoder.Close()
		}
		<-boundedDecoders.tickets
	}
}

func decodeBoundedFrame(decoder *zstd.Decoder, release func(bool), body []byte, size uint64) (raw []byte, err error) {
	reuse := false
	defer func() { release(reuse) }()
	raw, err = decoder.DecodeAll(body, make([]byte, 0, int(size)))
	reuse = err == nil
	return raw, err
}
