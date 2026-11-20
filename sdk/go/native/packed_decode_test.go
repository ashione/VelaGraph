package native

import (
	"encoding/binary"
	"math"
	"testing"
)

// Hand-derived single numeric-row fixture, without the production encoder.
func packedFixture() []byte {
	b := make([]byte, 104)
	copy(b, "VGQR0001")
	for at, n := range map[int]uint64{16: 1, 24: 1, 32: 104, 48: 1, 56: 104, 72: 1, 80: 3, 88: 7} {
		binary.LittleEndian.PutUint64(b[at:at+8], n)
	}
	return b
}
func TestPackedDecoderRejectsMalformedBoundsAndTags(t *testing.T) {
	good := packedFixture()
	v, e := newBatchView(good, nil)
	if e != nil {
		t.Fatal(e)
	}
	r, e := v.Materialize()
	if e != nil {
		t.Fatal(e)
	}
	if n, _ := r.Results[0].Rows[0].Value.Int64(); n != 7 {
		t.Fatal(r)
	}
	v.Close()
	for _, tc := range []struct {
		at int
		n  uint64
	}{{16, 0}, {16, 65}, {24, 10001}, {32, 0}, {40, 1}, {48, 2}, {56, 105}, {64, 1}, {72, 2}, {80, 10}, {96, 1}, {24, math.MaxUint64}} {
		b := packedFixture()
		binary.LittleEndian.PutUint64(b[tc.at:tc.at+8], tc.n)
		if _, e := newBatchView(b, nil); e == nil {
			t.Fatalf("accepted offset %d = %d", tc.at, tc.n)
		}
	}
	for _, tag := range []uint64{7, 9} {
		for _, pair := range [][2]uint64{{0, 1}, {104, 1}, {math.MaxUint64, 0}, {104, math.MaxUint64}} {
			b := packedFixture()
			binary.LittleEndian.PutUint64(b[80:88], tag)
			binary.LittleEndian.PutUint64(b[88:96], pair[0])
			binary.LittleEndian.PutUint64(b[96:104], pair[1])
			if _, e := newBatchView(b, nil); e == nil {
				t.Fatal("accepted bad span", tag, pair)
			}
		}
	}
	for _, tc := range []struct{ tag, bits uint64 }{{4, math.Float64bits(math.Inf(1))}, {4, math.Float64bits(math.NaN())}, {5, 2}, {6, 1}} {
		b := packedFixture()
		binary.LittleEndian.PutUint64(b[80:88], tc.tag)
		binary.LittleEndian.PutUint64(b[88:96], tc.bits)
		if _, e := newBatchView(b, nil); e == nil {
			t.Fatal(tc)
		}
	}
}
func FuzzPackedDecoder(f *testing.F) {
	f.Add(packedFixture())
	f.Add([]byte("VGQR0001"))
	f.Fuzz(func(t *testing.T, data []byte) {
		v, e := newBatchView(data, nil)
		if e != nil {
			return
		}
		defer v.Close()
		if _, e = v.Materialize(); e != nil {
			t.Fatal(e)
		}
	})
}

func TestPackedTextPathAndMetadataBoundaries(t *testing.T) {
	for _, tag := range []uint64{7, 9} {
		b := packedFixture()
		binary.LittleEndian.PutUint64(b[80:88], tag)
		binary.LittleEndian.PutUint64(b[88:96], 104)
		v, e := newBatchView(b, nil)
		if e != nil {
			t.Fatal(e)
		}
		r, e := v.Materialize()
		if e != nil {
			t.Fatal(e)
		}
		if tag == 7 {
			text, ok := r.Results[0].Rows[0].Value.String()
			if !ok || text != "" {
				t.Fatal(r)
			}
		} else {
			if r.Results[0].Rows[0].Path == nil || len(r.Results[0].Rows[0].Path) != 0 {
				t.Fatal(r)
			}
		}
		if r.Metadata == nil {
			t.Fatal("present-empty metadata lost")
		}
		v.Close()
	}
	b := append(packedFixture(), 0xff)
	binary.LittleEndian.PutUint64(b[56:64], 105)
	binary.LittleEndian.PutUint64(b[80:88], 7)
	binary.LittleEndian.PutUint64(b[88:96], 104)
	binary.LittleEndian.PutUint64(b[96:104], 1)
	if _, e := newBatchView(b, nil); e == nil {
		t.Fatal("invalid UTF-8 accepted")
	}
	b = append(packedFixture(), make([]byte, 16)...)
	binary.LittleEndian.PutUint64(b[56:64], 120)
	binary.LittleEndian.PutUint64(b[80:88], 9)
	binary.LittleEndian.PutUint64(b[88:96], 104)
	binary.LittleEndian.PutUint64(b[96:104], 1)
	binary.LittleEndian.PutUint64(b[104:112], 3)
	if _, e := newBatchView(b, nil); e == nil {
		t.Fatal("invalid path tag accepted")
	}
	b = packedFixture()
	binary.LittleEndian.PutUint64(b[48:56], 0)
	v, e := newBatchView(b, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer v.Close()
	r, e := v.Materialize()
	if e != nil || r.Metadata != nil {
		t.Fatal(r, e)
	}
	for n := 0; n < 104; n++ {
		if _, e := newBatchView(packedFixture()[:n], nil); e == nil {
			t.Fatal("truncated table accepted", n)
		}
	}
}

func TestPackedRejectsAliasedPayloadExpansion(t *testing.T) {
	// Two text rows alias one payload: accepting arbitrary shared spans permits
	// small native frames to amplify into huge owned Go allocations.
	b := make([]byte, 131)
	copy(b, "VGQR0001")
	for at, n := range map[int]uint64{16: 1, 24: 2, 32: 128, 48: 1, 56: 131, 72: 2, 80: 7, 88: 128, 96: 3, 104: 7, 112: 128, 120: 3} {
		binary.LittleEndian.PutUint64(b[at:at+8], n)
	}
	copy(b[128:], "abc")
	if _, e := newBatchView(b, nil); e == nil {
		t.Fatal("overlapping payloads accepted")
	}
}
