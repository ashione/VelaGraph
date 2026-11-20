//go:build !cgo || !velagraph_native

package native

import "context"

func Available() bool                                                    { return false }
func OpenMemory(context.Context, MemoryOptions) (*Store, error)          { return nil, ErrUnavailable }
func OpenTemporal(context.Context, string) (*Store, error)               { return nil, ErrUnavailable }
func Prepare(context.Context, string, PrepareOptions) (*Prepared, error) { return nil, ErrUnavailable }
func (*Store) Close() error                                              { return nil }
func (*Prepared) Close() error                                           { return nil }
func (*Store) Info(context.Context) (Info, error)                        { return Info{}, ErrUnavailable }
func (*Store) Apply(context.Context, WriteBatch) (uint64, error)         { return 0, ErrUnavailable }
func (*Store) Query(context.Context, []Query, QueryOptions) (BatchResult, error) {
	return BatchResult{}, ErrUnavailable
}
func (*Store) Checkpoint(context.Context, string) error { return ErrUnavailable }
func Stats() (RuntimeStats, error)                      { return RuntimeStats{}, ErrUnavailable }
