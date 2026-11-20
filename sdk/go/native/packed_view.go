package native

import (
	"encoding/binary"
	"math"
	"sync"
	"unicode/utf8"
)

const packedHeader = 64
const packedResult = 16
const packedRow = 24
const packedElement = 16
const packedMagic = "VGQR0001"

// RowKind is a scalar/identity/path discriminator in a native result view.
type RowKind uint64

const (
	KindVertex RowKind = 1 + iota
	KindEdge
	KindInteger
	KindReal
	KindBoolean
	KindNull
	KindText
	KindCount
	KindPath
)

// BatchView owns immutable native bytes. Close it explicitly. Copies share one
// owner; its methods and all derived RowViews reject access after any copy closes.
// No native byte slice or borrowed string is exposed to application code.
type BatchView struct{ owner *resultOwner }
type resultOwner struct {
	mu       sync.RWMutex
	data     []byte
	release  func() error
	rowStart uint64
}

// RowView is an immutable row reference sharing its BatchView lifetime.
type RowView struct {
	owner  *resultOwner
	offset uint64
}

func corruptPacked() error { return &Error{Code: CodeCorrupt, Message: "invalid packed result"} }
func wrongRow() error {
	return &Error{Code: CodeInvalidArgument, Message: "row kind or index mismatch"}
}
func packedWord(data []byte, offset uint64) uint64 {
	return binary.LittleEndian.Uint64(data[offset : offset+8])
}
func packedSpan(data []byte, offset, length uint64) bool {
	return offset <= uint64(len(data)) && length <= uint64(len(data))-offset
}
func newBatchView(data []byte, release func() error) (*BatchView, error) {
	rowStart, e := validatePacked(data)
	if e != nil {
		return nil, e
	}
	return &BatchView{owner: &resultOwner{data: data, release: release, rowStart: rowStart}}, nil
}
func validatePacked(data []byte) (uint64, error) {
	if len(data) < packedHeader || len(data) > MaxOutputBytes || string(data[:8]) != packedMagic {
		return 0, corruptPacked()
	}
	count, rows := packedWord(data, 16), packedWord(data, 24)
	if count == 0 || count > MaxQueries || rows > 10000 || packedWord(data, 48) > 1 || packedWord(data, 56) != uint64(len(data)) {
		return 0, corruptPacked()
	}
	rowStart := uint64(packedHeader) + count*packedResult
	payload := rowStart + rows*packedRow
	if !packedSpan(data, 0, payload) {
		return 0, corruptPacked()
	}
	offset, length := packedWord(data, 32), packedWord(data, 40)
	if offset != payload || length > 65536 || !packedSpan(data, offset, length) || (packedWord(data, 48) == 0 && length != 0) {
		return 0, corruptPacked()
	}
	var first uint64
	for i := uint64(0); i < count; i++ {
		at := uint64(packedHeader) + i*packedResult
		n := packedWord(data, at+8)
		if packedWord(data, at) != first || n > rows-first {
			return 0, corruptPacked()
		}
		first += n
	}
	if first != rows {
		return 0, corruptPacked()
	}
	payload += length
	for i := uint64(0); i < rows; i++ {
		at := rowStart + i*packedRow
		kind, a, b := RowKind(packedWord(data, at)), packedWord(data, at+8), packedWord(data, at+16)
		switch kind {
		case KindVertex, KindEdge, KindInteger, KindCount:
			if b != 0 {
				return 0, corruptPacked()
			}
		case KindReal:
			n := math.Float64frombits(a)
			if b != 0 || math.IsNaN(n) || math.IsInf(n, 0) {
				return 0, corruptPacked()
			}
		case KindBoolean:
			if a > 1 || b != 0 {
				return 0, corruptPacked()
			}
		case KindNull:
			if a != 0 || b != 0 {
				return 0, corruptPacked()
			}
		case KindText:
			if a != payload || !packedSpan(data, a, b) || !utf8.Valid(data[a:a+b]) {
				return 0, corruptPacked()
			}
			payload += b
		case KindPath:
			if a != payload || b > uint64(len(data))/packedElement || !packedSpan(data, a, b*packedElement) {
				return 0, corruptPacked()
			}
			for j := uint64(0); j < b; j++ {
				tag := packedWord(data, a+j*packedElement)
				if tag != uint64(KindVertex) && tag != uint64(KindEdge) {
					return 0, corruptPacked()
				}
			}
			payload += b * packedElement
		default:
			return 0, corruptPacked()
		}
	}
	if payload != uint64(len(data)) {
		return 0, corruptPacked()
	}
	return rowStart, nil
}
func (v *BatchView) lock() (*resultOwner, error) {
	if v == nil || v.owner == nil {
		return nil, ErrClosed
	}
	o := v.owner
	o.mu.RLock()
	if o.data == nil {
		o.mu.RUnlock()
		return nil, ErrClosed
	}
	return o, nil
}

// Close waits for active accessor calls, then releases the native lease once.
func (v *BatchView) Close() error {
	if v == nil || v.owner == nil {
		return nil
	}
	o := v.owner
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.data == nil {
		return nil
	}
	o.data = nil
	release := o.release
	o.release = nil
	if release != nil {
		return release()
	}
	return nil
}
func (v *BatchView) Version() (uint64, error) {
	o, e := v.lock()
	if e != nil {
		return 0, e
	}
	defer o.mu.RUnlock()
	return packedWord(o.data, 8), nil
}
func (v *BatchView) ResultCount() (int, error) {
	o, e := v.lock()
	if e != nil {
		return 0, e
	}
	defer o.mu.RUnlock()
	return int(packedWord(o.data, 16)), nil
}
func resultRange(o *resultOwner, result int) (uint64, uint64, error) {
	if result < 0 || uint64(result) >= packedWord(o.data, 16) {
		return 0, 0, wrongRow()
	}
	at := uint64(packedHeader) + uint64(result)*packedResult
	return packedWord(o.data, at), packedWord(o.data, at+8), nil
}
func (v *BatchView) RowCount(result int) (int, error) {
	o, e := v.lock()
	if e != nil {
		return 0, e
	}
	defer o.mu.RUnlock()
	_, n, e := resultRange(o, result)
	return int(n), e
}
func (v *BatchView) Row(result, index int) (RowView, error) {
	o, e := v.lock()
	if e != nil {
		return RowView{}, e
	}
	defer o.mu.RUnlock()
	first, n, e := resultRange(o, result)
	if e != nil {
		return RowView{}, e
	}
	if index < 0 || uint64(index) >= n {
		return RowView{}, wrongRow()
	}
	return RowView{owner: o, offset: o.rowStart + (first+uint64(index))*packedRow}, nil
}

// Metadata returns an owned copy, preserving absent versus present-empty metadata.
func (v *BatchView) Metadata() ([]byte, error) {
	o, e := v.lock()
	if e != nil {
		return nil, e
	}
	defer o.mu.RUnlock()
	return metadataOwned(o.data), nil
}
func metadataOwned(data []byte) []byte {
	if packedWord(data, 48) == 0 {
		return nil
	}
	at, n := packedWord(data, 32), packedWord(data, 40)
	out := make([]byte, n)
	copy(out, data[at:at+n])
	return out
}
func (v RowView) lock() (*resultOwner, error) {
	if v.owner == nil {
		return nil, ErrClosed
	}
	o := v.owner
	o.mu.RLock()
	if o.data == nil {
		o.mu.RUnlock()
		return nil, ErrClosed
	}
	return o, nil
}
func (v RowView) words() (RowKind, uint64, uint64, error) {
	o, e := v.lock()
	if e != nil {
		return 0, 0, 0, e
	}
	defer o.mu.RUnlock()
	return RowKind(packedWord(o.data, v.offset)), packedWord(o.data, v.offset+8), packedWord(o.data, v.offset+16), nil
}
func (v RowView) Kind() (RowKind, error) { k, _, _, e := v.words(); return k, e }
func (v RowView) ID() (uint64, error) {
	k, a, _, e := v.words()
	if e != nil {
		return 0, e
	}
	if k != KindVertex && k != KindEdge {
		return 0, wrongRow()
	}
	return a, nil
}
func (v RowView) Count() (uint64, error) {
	k, a, _, e := v.words()
	if e != nil {
		return 0, e
	}
	if k != KindCount {
		return 0, wrongRow()
	}
	return a, nil
}
func (v RowView) Int64() (int64, error) {
	k, a, _, e := v.words()
	if e != nil {
		return 0, e
	}
	if k != KindInteger {
		return 0, wrongRow()
	}
	return int64(a), nil
}
func (v RowView) Float64() (float64, error) {
	k, a, _, e := v.words()
	if e != nil {
		return 0, e
	}
	if k != KindReal {
		return 0, wrongRow()
	}
	return math.Float64frombits(a), nil
}
func (v RowView) Bool() (bool, error) {
	k, a, _, e := v.words()
	if e != nil {
		return false, e
	}
	if k != KindBoolean {
		return false, wrongRow()
	}
	return a == 1, nil
}
func (v RowView) text(o *resultOwner) ([]byte, error) {
	if RowKind(packedWord(o.data, v.offset)) != KindText {
		return nil, wrongRow()
	}
	at, n := packedWord(o.data, v.offset+8), packedWord(o.data, v.offset+16)
	return o.data[at : at+n], nil
}

// Text copies into a Go-owned string that remains valid after Close.
func (v RowView) Text() (string, error) {
	o, e := v.lock()
	if e != nil {
		return "", e
	}
	defer o.mu.RUnlock()
	b, e := v.text(o)
	if e != nil {
		return "", e
	}
	return string(b), nil
}

// TextEquals compares native UTF-8 bytes directly without allocating/copying a string.
func (v RowView) TextEquals(s string) (bool, error) {
	o, e := v.lock()
	if e != nil {
		return false, e
	}
	defer o.mu.RUnlock()
	b, e := v.text(o)
	if e != nil {
		return false, e
	}
	if len(b) != len(s) {
		return false, nil
	}
	for i, c := range b {
		if c != s[i] {
			return false, nil
		}
	}
	return true, nil
}
func (v RowView) PathLen() (int, error) {
	k, _, n, e := v.words()
	if e != nil {
		return 0, e
	}
	if k != KindPath {
		return 0, wrongRow()
	}
	return int(n), nil
}
func (v RowView) PathElement(index int) (PathElement, error) {
	o, e := v.lock()
	if e != nil {
		return PathElement{}, e
	}
	defer o.mu.RUnlock()
	k, at, n := RowKind(packedWord(o.data, v.offset)), packedWord(o.data, v.offset+8), packedWord(o.data, v.offset+16)
	if k != KindPath || index < 0 || uint64(index) >= n {
		return PathElement{}, wrongRow()
	}
	return pathOwned(o.data, at+uint64(index)*packedElement), nil
}
func pathOwned(data []byte, at uint64) PathElement {
	kind := "Vertex"
	if packedWord(data, at) == uint64(KindEdge) {
		kind = "Edge"
	}
	return PathElement{Kind: kind, ID: packedWord(data, at+8)}
}
func rowOwned(data []byte, at uint64) Row {
	k, a, b := RowKind(packedWord(data, at)), packedWord(data, at+8), packedWord(data, at+16)
	switch k {
	case KindVertex:
		return Row{Kind: "Vertex", ID: a}
	case KindEdge:
		return Row{Kind: "Edge", ID: a}
	case KindCount:
		return Row{Kind: "Count", Count: a}
	case KindInteger:
		return Row{Kind: "Value", Value: Integer(int64(a))}
	case KindReal:
		return Row{Kind: "Value", Value: Real(math.Float64frombits(a))}
	case KindBoolean:
		return Row{Kind: "Value", Value: Boolean(a == 1)}
	case KindNull:
		return Row{Kind: "Value", Value: Null()}
	case KindText:
		return Row{Kind: "Value", Value: Text(string(data[a : a+b]))}
	case KindPath:
		path := make([]PathElement, b)
		for i := range path {
			path[i] = pathOwned(data, a+uint64(i)*packedElement)
		}
		return Row{Kind: "Path", Path: path}
	}
	panic("validated packed row has invalid kind")
}

// Materialize returns the same owned result as Query; no returned value depends
// on the lease after this method returns. This deliberately allocates Go rows.
func (v *BatchView) Materialize() (BatchResult, error) {
	o, e := v.lock()
	if e != nil {
		return BatchResult{}, e
	}
	defer o.mu.RUnlock()
	result := BatchResult{Version: packedWord(o.data, 8), Metadata: metadataOwned(o.data), Results: make([]Result, packedWord(o.data, 16))}
	for i := range result.Results {
		first, n, _ := resultRange(o, i)
		rows := make([]Row, n)
		for j := range rows {
			rows[j] = rowOwned(o.data, o.rowStart+(first+uint64(j))*packedRow)
		}
		result.Results[i].Rows = rows
	}
	return result, nil
}
