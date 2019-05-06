package versions

import (
	"io"

	"github.com/evandigby/kafka/api"
)

type Request struct{}

func (r *Request) Size() int32             { return 0 }
func (r *Request) Write(w io.Writer) error { return nil }
func (r *Request) APIKey() api.Key         { return api.KeyAPIVersions }
func (r *Request) Version() int16          { return 1 }
