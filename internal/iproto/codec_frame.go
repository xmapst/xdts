package iproto

import "github.com/klauspost/compress/zstd"

// Use declared output capacity only for one complete ordinary frame. Other
// inputs retain streaming validation, including skippable and concatenated
// frames and frames without content size. Decoder still validates the content.
func singleFrameContentSize(body []byte) (uint64, bool) {
	var header zstd.Header
	if header.Decode(body) != nil || !header.HasFCS || header.Skippable {
		return 0, false
	}
	pos := header.HeaderSize
	for {
		if len(body)-pos < 3 {
			return 0, false
		}
		block := uint32(body[pos]) | uint32(body[pos+1])<<8 | uint32(body[pos+2])<<16
		pos += 3
		kind := block >> 1 & 3
		size := int(block >> 3)
		if kind == 3 {
			return 0, false
		}
		if kind == 1 {
			size = 1
		}
		if size > len(body)-pos {
			return 0, false
		}
		pos += size
		if block&1 != 0 {
			break
		}
	}
	if header.HasCheckSum {
		if len(body)-pos < 4 {
			return 0, false
		}
		pos += 4
	}
	return header.FrameContentSize, pos == len(body)
}
