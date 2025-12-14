package ocf

import (
	"math/rand"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestZstdEncodeDecodeLowEntropyLong(t *testing.T) {
	input := makeTestData(8762, func() byte { return 'a' })

	verifyZstdEncodeDecode(t, input)
}

func TestZstdEncodeDecodeLowEntropyShort(t *testing.T) {
	input := makeTestData(7, func() byte { return 'a' })

	verifyZstdEncodeDecode(t, input)
}

func TestZstdEncodeDecodeHighEntropyLong(t *testing.T) {
	input := makeTestData(8762, func() byte { return byte(rand.Uint32()) })

	verifyZstdEncodeDecode(t, input)
}

func TestZstdEncodeDecodeHighEntropyShort(t *testing.T) {
	input := makeTestData(7, func() byte { return byte(rand.Uint32()) })

	verifyZstdEncodeDecode(t, input)
}

/*
benchmark results always creating a new zstd encoder/decoder

goos: linux
goarch: amd64
pkg: github.com/aryehlev/avro/v2/ocf
cpu: AMD Ryzen 5 3550H with Radeon Vega Mobile Gfx


BenchmarkZstdEncodeDecodeLowEntropyLong
BenchmarkZstdEncodeDecodeLowEntropyLong-8    	     289	   3523847 ns/op	10891887 B/op	      40 allocs/op
BenchmarkZstdEncodeDecodeHighEntropyLong
BenchmarkZstdEncodeDecodeHighEntropyLong-8   	     298	   3390952 ns/op	10894703 B/op	      40 allocs/op


benchmark results reusing an existing zstd encoder/decoder

BenchmarkZstdEncodeDecodeLowEntropyLong
BenchmarkZstdEncodeDecodeLowEntropyLong-8    	   55628	     22883 ns/op	   19220 B/op	       2 allocs/op
BenchmarkZstdEncodeDecodeHighEntropyLong
BenchmarkZstdEncodeDecodeHighEntropyLong-8   	   47652	     25064 ns/op	   31553 B/op	       3 allocs/op
*/

func BenchmarkZstdEncodeDecodeLowEntropyLong(b *testing.B) {
	input := makeTestData(8762, func() byte { return 'a' })

	encoder, err := resolveCodec(ZStandard, codecOptions{}, codecModeEncode)
	require.NoError(b, err)
	decoder, err := resolveCodec(ZStandard, codecOptions{}, codecModeDecode)
	require.NoError(b, err)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		compressed := encoder.Encode(input)
		_, decodeErr := decoder.Decode(compressed)
		require.NoError(b, decodeErr)
	}
}

func BenchmarkZstdEncodeDecodeHighEntropyLong(b *testing.B) {
	input := makeTestData(8762, func() byte { return byte(rand.Uint32()) })

	encoder, err := resolveCodec(ZStandard, codecOptions{}, codecModeEncode)
	require.NoError(b, err)
	decoder, err := resolveCodec(ZStandard, codecOptions{}, codecModeDecode)
	require.NoError(b, err)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		compressed := encoder.Encode(input)
		_, decodeErr := decoder.Decode(compressed)
		require.NoError(b, decodeErr)
	}
}

func verifyZstdEncodeDecode(t *testing.T, input []byte) {
	encoder, err := resolveCodec(ZStandard, codecOptions{}, codecModeEncode)
	require.NoError(t, err)
	decoder, err := resolveCodec(ZStandard, codecOptions{}, codecModeDecode)
	require.NoError(t, err)

	compressed := encoder.Encode(input)
	actual, decodeErr := decoder.Decode(compressed)

	require.NoError(t, decodeErr)
	assert.Equal(t, input, actual)
}

func makeTestData(length int, charMaker func() byte) []byte {
	input := make([]byte, length)
	for i := 0; i < length; i++ {
		input[i] = charMaker()
	}
	return input
}

func TestZstdSharedEncoder(t *testing.T) {
	input := makeTestData(8762, func() byte { return 'a' })

	// Create shared encoder/decoder
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	require.NoError(t, err)
	defer enc.Close()

	dec, err := zstd.NewReader(nil)
	require.NoError(t, err)
	defer dec.Close()

	// Create encoder codecs sharing the same zstd encoder
	encoder1, err := resolveCodec(ZStandard, codecOptions{
		ZStandardOptions: zstdOptions{Encoder: enc},
	}, codecModeEncode)
	require.NoError(t, err)

	encoder2, err := resolveCodec(ZStandard, codecOptions{
		ZStandardOptions: zstdOptions{Encoder: enc},
	}, codecModeEncode)
	require.NoError(t, err)

	// Create decoder codecs sharing the same zstd decoder
	decoder1, err := resolveCodec(ZStandard, codecOptions{
		ZStandardOptions: zstdOptions{Decoder: dec},
	}, codecModeDecode)
	require.NoError(t, err)

	decoder2, err := resolveCodec(ZStandard, codecOptions{
		ZStandardOptions: zstdOptions{Decoder: dec},
	}, codecModeDecode)
	require.NoError(t, err)

	// Both encoders should work correctly
	compressed1 := encoder1.Encode(input)
	compressed2 := encoder2.Encode(input)

	actual1, err := decoder1.Decode(compressed1)
	require.NoError(t, err)
	assert.Equal(t, input, actual1)

	actual2, err := decoder2.Decode(compressed2)
	require.NoError(t, err)
	assert.Equal(t, input, actual2)

	// Cross-decode should also work
	actual3, err := decoder1.Decode(compressed2)
	require.NoError(t, err)
	assert.Equal(t, input, actual3)

	// Closing codecs should not close the shared encoder/decoder
	encoder1.(*ZStandardCodec).Close()
	encoder2.(*ZStandardCodec).Close()
	decoder1.(*ZStandardCodec).Close()
	decoder2.(*ZStandardCodec).Close()

	// Shared encoder/decoder should still work after codec close
	compressed3 := enc.EncodeAll(input, nil)
	actual4, err := dec.DecodeAll(compressed3, nil)
	require.NoError(t, err)
	assert.Equal(t, input, actual4)
}
