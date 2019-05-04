package kafka

import (
	"bytes"
	"sync"
)

var buffers = sync.Pool{
	New: func() interface{} {
		return &byteBuffer{
			Buffer: bytes.Buffer{},
		}
	},
}

func newBuffer() *byteBuffer {
	return buffers.Get().(*byteBuffer)
}

type byteBuffer struct {
	bytes.Buffer
}

func (b *byteBuffer) Close() {
	b.Reset()
	buffers.Put(b)
}
