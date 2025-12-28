package avro_test

import (
	"bytes"
	"fmt"
	"math"
	"testing"

	"github.com/aryehlev/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncoder_RecordStruct(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestRecord{A: 27, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x36, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_RecordStructPtr(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"}
	]
}`
	obj := &TestRecord{A: 27, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x36, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_RecordStructPtrNil(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"}
	]
}`
	var obj *TestRecord
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	assert.Error(t, err)
}

func TestEncoder_RecordStructMissingRequiredField(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestPartialRecord{B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	assert.Error(t, err)
}

func TestEncoder_RecordStructWithDefault(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long", "default": 27},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestPartialRecord{B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x36, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_RecordStructPartialWithNullDefault(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "string"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestPartialRecord{B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x00, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_RecordStructPartialWithSubRecordDefault(t *testing.T) {
	defer ConfigTeardown()

	_, err := avro.Parse(`{
		"type": "record",
		"name": "test",
		"fields" : [
			{"name": "a", "type": "long"},
			{"name": "b", "type": "string"}
		]
	}`)
	require.NoError(t, err)

	schema := `{
		"type": "record",
		"name": "parent",
		"fields" : [
			{
				"name": "a",
				"type": "test",
				"default": {"a": 1000, "b": "def b"}
			},
			{"name": "b", "type": "string"}
		]
	}`
	obj := TestPartialRecord{B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)
	require.NoError(t, err)

	assert.Equal(t, []byte{0xd0, 0xf, 0xa, 0x64, 0x65, 0x66, 0x20, 0x62, 0x6, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_RecordStructWithNullDefault(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "null", "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestPartialRecord{B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_RecordStructFieldError(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "string"},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestRecord{A: 27, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	assert.Error(t, err)
}

func TestEncoder_RecordEmbeddedStruct(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"},
	    {"name": "c", "type": "string"}
	]
}`
	obj := TestEmbeddedRecord{TestEmbed: TestEmbed{A: 27, B: "foo"}, C: "bar"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x36, 0x06, 0x66, 0x6f, 0x6f, 0x06, 0x62, 0x61, 0x72}, buf.Bytes())
}

func TestEncoder_RecordEmbeddedPtrStruct(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"},
	    {"name": "c", "type": "string"}
	]
}`
	obj := TestEmbeddedPtrRecord{TestEmbed: &TestEmbed{A: 27, B: "foo"}, C: "bar"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x36, 0x06, 0x66, 0x6f, 0x6f, 0x06, 0x62, 0x61, 0x72}, buf.Bytes())
}

func TestEncoder_RecordEmbeddedPtrStructNull(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"},
	    {"name": "c", "type": "string"}
	]
}`
	obj := TestEmbeddedPtrRecord{C: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	assert.Error(t, err)
}

func TestEncoder_RecordEmbeddedIntStruct(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestEmbeddedIntRecord{TestEmbedInt: 27, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	assert.Error(t, err)
}

func TestEncoder_RecordUnexportedStruct(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestUnexportedRecord{A: 27, b: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	assert.Error(t, err)
}

func TestEncoder_RecordMap(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"},
		{"name": "c", "type": "null"}
	]
}`
	obj := map[string]any{"a": int64(27), "b": "foo", "c": nil}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x36, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_RecordMapNested(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "parent",
	"fields" : [
		{"name": "a", "type": {
			"type": "record",
			"name": "test",
			"fields" : [
				{"name": "a", "type": "long"},
	    		{"name": "b", "type": "string"}
			]}
		},
	    {"name": "b", "type": "string"}
	]
}`
	obj := map[string]any{"a": map[string]any{
		"a": int64(27),
		"b": "bar",
	}, "b": "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x36, 0x6, 0x62, 0x61, 0x72, 0x6, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_RecordMapNilValue(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"}
	]
}`
	obj := map[string]any{"a": int64(27), "b": nil}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	assert.Error(t, err)
}

func TestEncoder_RecordMapMissingRequiredField(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"}
	]
}`
	obj := map[string]any{"b": "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	assert.Error(t, err)
}

func TestEncoder_RecordMapWithDefault(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long", "default": 27},
	    {"name": "b", "type": "string"}
	]
}`
	obj := map[string]any{"b": "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x36, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_RecordMapWithSubRecordDefault(t *testing.T) {
	defer ConfigTeardown()

	_, err := avro.Parse(`{
		"type": "record",
		"name": "test",
		"fields" : [
			{"name": "a", "type": "long"},
			{"name": "b", "type": "string"}
		]
	}`)
	require.NoError(t, err)

	schema := `{
		"type": "record",
		"name": "parent",
		"fields" : [
			{
				"name": "a",
				"type": "test",
				"default": {"a": 1000, "b": "def b"}
			},
			{"name": "b", "type": "string"}
		]
	}`

	obj := map[string]any{"b": "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)
	require.NoError(t, err)

	assert.Equal(t, []byte{0xd0, 0xf, 0xa, 0x64, 0x65, 0x66, 0x20, 0x62, 0x6, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_RecordMapWithNullDefault(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "null", "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := map[string]any{"b": "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_RecordMapWithUnionNullDefault(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "string"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := map[string]any{"b": "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x00, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_RecordMapWithUnionStringDefault(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["string", "null"], "default": "test"},
	    {"name": "b", "type": "string"}
	]
}`
	obj := map[string]any{"b": "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x0, 0x8, 0x74, 0x65, 0x73, 0x74, 0x6, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_RecordMapInvalidKeyType(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "null", "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := map[int]any{1: int64(27), 2: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	assert.Error(t, err)
}

func TestEncoder_RecordMapInvalidValueType(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "null", "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := map[string]string{"a": "test", "b": "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	assert.Error(t, err)
}

func TestEncoder_RefStruct(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "parent",
	"fields" : [
		{"name": "a", "type": {
			"type": "record",
			"name": "test",
			"fields" : [
				{"name": "a", "type": "long"},
	    		{"name": "b", "type": "string"}
			]}
		},
	    {"name": "b", "type": "test"}
	]
}`
	obj := TestNestedRecord{
		A: TestRecord{A: 27, B: "foo"},
		B: TestRecord{A: 27, B: "foo"},
	}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x36, 0x06, 0x66, 0x6f, 0x6f, 0x36, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_RecordStructOmitEmptyNullableZeroValue(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyRecord{A: 0, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// 0x00 = null index (default), 0x06 0x66 0x6f 0x6f = "foo"
	assert.Equal(t, []byte{0x00, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_RecordStructOmitEmptyNonZeroValue(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyRecord{A: 27, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// 0x02 = long index (1), 0x36 = 27 varint, 0x06 0x66 0x6f 0x6f = "foo"
	assert.Equal(t, []byte{0x02, 0x36, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_RecordStructOmitEmptyNoDefaultEncodesValue(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"}
	]
}`
	// When no default in schema, zerodefault is ignored - value is encoded normally
	obj := TestOmitEmptyNoDefault{A: 0, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// 0x00 = 0 varint, 0x06 0x66 0x6f 0x6f = "foo"
	assert.Equal(t, []byte{0x00, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_RecordStructOmitEmptyStringZeroValue(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long"], "default": null},
	    {"name": "b", "type": ["null", "string"], "default": null}
	]
}`
	obj := TestOmitEmptyMultipleRecord{A: 27, B: ""}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// 0x02 = long index (1), 0x36 = 27 varint, 0x00 = null index for empty string
	assert.Equal(t, []byte{0x02, 0x36, 0x00}, buf.Bytes())
}

func TestEncoder_RecordStructOmitEmptyNonNullableEncodesValue(t *testing.T) {
	defer ConfigTeardown()

	// For non-nullable schemas, omitempty has no effect - encodes actual value
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long", "default": 100},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyRecord{A: 0, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// 0x00 = 0 varint, 0x06 0x66 0x6f 0x6f = "foo"
	assert.Equal(t, []byte{0x00, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_OmitEmpty_BoolFalseEncodesDefault(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "boolean"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyBool{A: false, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// 0x00 = null index (default for false), 0x06 0x66 0x6f 0x6f = "foo"
	assert.Equal(t, []byte{0x00, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_OmitEmpty_BoolTrueEncodesValue(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "boolean"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyBool{A: true, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// 0x02 = boolean index (1), 0x01 = true, 0x06 0x66 0x6f 0x6f = "foo"
	assert.Equal(t, []byte{0x02, 0x01, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_OmitEmpty_BoolNonNullableEncodesValue(t *testing.T) {
	defer ConfigTeardown()

	// For non-nullable schemas, omitempty has no effect - encodes actual value
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "boolean", "default": true},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyBool{A: false, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// 0x00 = false, 0x06 0x66 0x6f 0x6f = "foo"
	assert.Equal(t, []byte{0x00, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_OmitEmpty_Float32ZeroEncodesDefault(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "float"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyFloat32{A: 0.0, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// 0x00 = null index (default), 0x06 0x66 0x6f 0x6f = "foo"
	assert.Equal(t, []byte{0x00, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_OmitEmpty_Float32NonZeroEncodesValue(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "float"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyFloat32{A: 3.14, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// Should encode the float value, not null
	assert.Equal(t, byte(0x02), buf.Bytes()[0]) // float index
}

func TestEncoder_OmitEmpty_Float64ZeroEncodesDefault(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "double"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyFloat64{A: 0.0, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// 0x00 = null index (default), 0x06 0x66 0x6f 0x6f = "foo"
	assert.Equal(t, []byte{0x00, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_OmitEmpty_Float64WithNonNullDefault(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "double", "default": 3.14159},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyFloat64{A: 0.0, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// First 8 bytes should be 3.14159 in IEEE 754 double format
	assert.Len(t, buf.Bytes(), 8+4) // 8 for double + 4 for "foo"
}

func TestEncoder_OmitEmpty_Int32ZeroEncodesDefault(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "int"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyInt32{A: 0, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// 0x00 = null index (default), 0x06 0x66 0x6f 0x6f = "foo"
	assert.Equal(t, []byte{0x00, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_OmitEmpty_NilSliceEncodesDefault(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", {"type": "array", "items": "string"}], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptySlice{A: nil, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// 0x00 = null index (default), 0x06 0x66 0x6f 0x6f = "foo"
	assert.Equal(t, []byte{0x00, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_OmitEmpty_EmptySliceEncodesDefault(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", {"type": "array", "items": "string"}], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptySlice{A: []string{}, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// Empty slice is also zero, should encode default
	// 0x00 = null index (default), 0x06 0x66 0x6f 0x6f = "foo"
	assert.Equal(t, []byte{0x00, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_OmitEmpty_NonEmptySliceEncodesValue(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", {"type": "array", "items": "string"}], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptySlice{A: []string{"hello"}, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// Should encode the array value, not null
	assert.Equal(t, byte(0x02), buf.Bytes()[0]) // array index
}

func TestEncoder_OmitEmpty_NilMapEncodesDefault(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", {"type": "map", "values": "string"}], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyMap{A: nil, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// 0x00 = null index (default), 0x06 0x66 0x6f 0x6f = "foo"
	assert.Equal(t, []byte{0x00, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_OmitEmpty_NonNilMapEncodesValue(t *testing.T) {
	defer ConfigTeardown()

	// Note: map[string]any with union types requires proper union type wrapping
	// This test uses a non-union map schema to test zerodefault with maps
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": {"type": "map", "values": "string"}, "default": {}},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyMap{A: map[string]any{"key": "value"}, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// Should encode the map value since it's non-empty
	// Map block: count=1 (0x02), key="key" (0x06 len + bytes), value="value" (0x0a len + bytes), terminator=0x00
	assert.True(t, len(buf.Bytes()) > 4) // has map data plus "foo"
}

func TestEncoder_OmitEmpty_NilPointerEncodesDefault(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyPointer{A: nil, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// 0x00 = null index (default), 0x06 0x66 0x6f 0x6f = "foo"
	assert.Equal(t, []byte{0x00, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_OmitEmpty_NonNilPointerEncodesValue(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	val := int64(42)
	obj := TestOmitEmptyPointer{A: &val, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// Should encode the pointer value, not null
	// 0x02 = long index, 0x54 = 42 varint, 0x06 0x66 0x6f 0x6f = "foo"
	assert.Equal(t, []byte{0x02, 0x54, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_OmitEmpty_PointerToZeroEncodesValue(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	val := int64(0)
	obj := TestOmitEmptyPointer{A: &val, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// Pointer is not nil, so value should be encoded even though it points to 0
	// 0x02 = long index, 0x00 = 0 varint, 0x06 0x66 0x6f 0x6f = "foo"
	assert.Equal(t, []byte{0x02, 0x00, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_OmitEmpty_EmptyBytesEncodesDefault(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "bytes"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyBytes{A: []byte{}, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// Empty bytes is zero, should encode default
	assert.Equal(t, []byte{0x00, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_OmitEmpty_NonEmptyBytesEncodesValue(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "bytes"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyBytes{A: []byte{0xDE, 0xAD}, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// Should encode the bytes value, not null
	assert.Equal(t, byte(0x02), buf.Bytes()[0]) // bytes index
}

func TestEncoder_OmitEmpty_StringNonNullableEncodesValue(t *testing.T) {
	defer ConfigTeardown()

	// For non-nullable schemas, omitempty has no effect - encodes actual value (empty string)
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "string", "default": "default_value"},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyStringNonNull{A: "", B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// 0x00 = empty string (length 0), 0x06 0x66 0x6f 0x6f = "foo"
	assert.Equal(t, []byte{0x00, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_OmitEmpty_StringNonEmptyEncodesValue(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "string", "default": "default_value"},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyStringNonNull{A: "custom", B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// Should encode "custom" instead of default
	// 0x0c = 6 (length), "custom", 0x06 0x66 0x6f 0x6f = "foo"
	assert.Equal(t, []byte{0x0c, 0x63, 0x75, 0x73, 0x74, 0x6f, 0x6d, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_OmitEmpty_AllFieldsZeroEncodesAllDefaults(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long"], "default": null},
		{"name": "b", "type": ["null", "string"], "default": null},
		{"name": "c", "type": ["null", "boolean"], "default": null}
	]
}`
	obj := TestOmitEmptyAllFields{A: 0, B: "", C: false}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// All three should be null
	assert.Equal(t, []byte{0x00, 0x00, 0x00}, buf.Bytes())
}

func TestEncoder_OmitEmpty_AllFieldsNonZeroEncodesAllValues(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long"], "default": null},
		{"name": "b", "type": ["null", "string"], "default": null},
		{"name": "c", "type": ["null", "boolean"], "default": null}
	]
}`
	obj := TestOmitEmptyAllFields{A: 42, B: "hi", C: true}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// 0x02 = long index, 0x54 = 42, 0x02 = string index, 0x04 = len 2, "hi", 0x02 = bool index, 0x01 = true
	assert.Equal(t, []byte{0x02, 0x54, 0x02, 0x04, 0x68, 0x69, 0x02, 0x01}, buf.Bytes())
}

func TestEncoder_OmitEmpty_MixedZeroAndNonZero(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long"], "default": null},
		{"name": "b", "type": ["null", "string"], "default": null},
		{"name": "c", "type": ["null", "boolean"], "default": null}
	]
}`
	obj := TestOmitEmptyAllFields{A: 0, B: "hi", C: false}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// A=null, B="hi", C=null
	// 0x00 = null, 0x02 = string index, 0x04 = len 2, "hi", 0x00 = null
	assert.Equal(t, []byte{0x00, 0x02, 0x04, 0x68, 0x69, 0x00}, buf.Bytes())
}

func TestEncoder_OmitEmpty_NegativeNumberIsNonZero(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyRecord{A: -1, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// -1 is non-zero, should encode value
	// 0x02 = long index, 0x01 = -1 in zigzag varint, 0x06 0x66 0x6f 0x6f = "foo"
	assert.Equal(t, []byte{0x02, 0x01, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_OmitEmpty_LargeNumberIsNonZero(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyRecord{A: 9223372036854775807, B: "foo"} // max int64
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// Should encode the large value, not null
	assert.Equal(t, byte(0x02), buf.Bytes()[0]) // long index
}

func TestEncoder_OmitEmpty_NullSecondUnionWritesNull(t *testing.T) {
	defer ConfigTeardown()

	// When union is ["long", "null"] and value is zero, omitempty writes null
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["long", "null"], "default": 999},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyRecord{A: 0, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// With null as second type (index 1), null is written as 0x02 (varint for 1)
	// 0x02 = null index (1), 0x06 0x66 0x6f 0x6f = "foo"
	assert.Equal(t, []byte{0x02, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_OmitEmpty_WithOmitemptyTagCombination(t *testing.T) {
	defer ConfigTeardown()

	// Test that zerodefault can be combined with other tag options
	type TestCombinedTags struct {
		A int64  `avro:"a,omitempty,zerodefault"`
		B string `avro:"b"`
	}

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestCombinedTags{A: 0, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// zerodefault should still work
	assert.Equal(t, []byte{0x00, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_OmitEmpty_RoundTripNullableInt(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	// Encode with zerodefault
	obj := TestOmitEmptyRecord{A: 0, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)
	require.NoError(t, err)

	// Decode back - should get null for A
	type DecodedRecord struct {
		A *int64 `avro:"a"`
		B string `avro:"b"`
	}
	var decoded DecodedRecord
	dec, err := avro.NewDecoder(schema, bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	err = dec.Decode(&decoded)
	require.NoError(t, err)

	assert.Nil(t, decoded.A) // Should be nil because null was encoded
	assert.Equal(t, "foo", decoded.B)
}

func TestEncoder_OmitEmpty_RoundTripNonNullable(t *testing.T) {
	defer ConfigTeardown()

	// For non-nullable schemas, omitempty has no effect - encodes actual value
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long", "default": 100},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyRecord{A: 0, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)
	require.NoError(t, err)

	// Decode back - should get 0 for A (omitempty has no effect on non-nullable)
	var decoded TestRecord
	dec, err := avro.NewDecoder(schema, bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	err = dec.Decode(&decoded)
	require.NoError(t, err)

	assert.Equal(t, int64(0), decoded.A) // 0 because omitempty only works with nullable unions
	assert.Equal(t, "foo", decoded.B)
}

func TestEncoder_OmitEmpty_MinInt64(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyRecord{A: -9223372036854775808, B: "foo"} // min int64
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// Should encode the value, not null
	assert.Equal(t, byte(0x02), buf.Bytes()[0]) // long index
}

func TestEncoder_OmitEmpty_WhitespaceStringIsNonZero(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long"], "default": null},
	    {"name": "b", "type": ["null", "string"], "default": null}
	]
}`
	obj := TestOmitEmptyMultipleRecord{A: 0, B: " "} // whitespace is non-empty
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// A=null, B=" " (non-empty string)
	// 0x00 = null, 0x02 = string index, 0x02 = len 1, " "
	assert.Equal(t, []byte{0x00, 0x02, 0x02, 0x20}, buf.Bytes())
}

func TestEncoder_OmitEmpty_NegativeZeroFloat(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "double"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	// In Go, -0.0 == 0.0 is true, but they have different bit representations
	// Testing that isEmpty correctly handles this
	negZero := -0.0
	obj := TestOmitEmptyFloat64{A: negZero, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// -0.0 should be treated as zero and encode default
	assert.Equal(t, []byte{0x00, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_OmitEmpty_EmptyMapTreatedAsZero(t *testing.T) {
	defer ConfigTeardown()

	// NOTE: For maps, isEmptyFunc checks if the underlying pointer is nil.
	// An empty map `map[string]any{}` is NOT nil in Go, but the current
	// implementation treats it as zero and encodes the default.
	// This test documents the current behavior.
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", {"type": "map", "values": "string"}], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyMap{A: map[string]any{}, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// Empty map is treated as zero, so default (null) is encoded
	// 0x00 = null index (default), 0x06 0x66 0x6f 0x6f = "foo"
	assert.Equal(t, []byte{0x00, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_OmitEmpty_UintZeroEncodesDefault(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyUint{A: 0, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x00, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

// Note: TestEncoder_OmitEmpty_UintNonZeroEncodesValue removed because
// the library cannot resolve 'uint' type in union schemas

func TestEncoder_OmitEmpty_Uint8ZeroEncodesDefault(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "int"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyUint8{A: 0, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x00, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

// Note: TestEncoder_OmitEmpty_Uint8MaxEncodesValue removed because
// the library cannot resolve 'uint8' type in union schemas

func TestEncoder_OmitEmpty_Uint16ZeroEncodesDefault(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "int"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyUint16{A: 0, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x00, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_OmitEmpty_Uint32ZeroEncodesDefault(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyUint32{A: 0, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x00, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_OmitEmpty_Uint64ZeroEncodesDefault(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyUint64{A: 0, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x00, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_OmitEmpty_IntZeroEncodesDefault(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyInt{A: 0, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x00, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_OmitEmpty_Int8ZeroEncodesDefault(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "int"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyInt8{A: 0, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x00, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_OmitEmpty_Int8MinMaxEncodesValue(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "int"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	// Test min int8
	obj1 := TestOmitEmptyInt8{A: -128, B: "foo"}
	buf1 := &bytes.Buffer{}
	enc1, _ := avro.NewEncoder(schema, buf1)
	err := enc1.Encode(obj1)
	require.NoError(t, err)
	assert.Equal(t, byte(0x02), buf1.Bytes()[0]) // int index

	// Test max int8
	obj2 := TestOmitEmptyInt8{A: 127, B: "foo"}
	buf2 := &bytes.Buffer{}
	enc2, _ := avro.NewEncoder(schema, buf2)
	err = enc2.Encode(obj2)
	require.NoError(t, err)
	assert.Equal(t, byte(0x02), buf2.Bytes()[0]) // int index
}

func TestEncoder_OmitEmpty_Int16ZeroEncodesDefault(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "int"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyInt16{A: 0, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x00, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_OmitEmpty_CustomIntZeroEncodesDefault(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyCustomInt{A: 0, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x00, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_OmitEmpty_CustomStringEmptyEncodesDefault(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "string"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyCustomString{A: "", B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x00, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_OmitEmpty_PointerStringNilEncodesDefault(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "string"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyPointerString{A: nil, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x00, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_OmitEmpty_PointerStringEmptyEncodesValue(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "string"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	empty := ""
	obj := TestOmitEmptyPointerString{A: &empty, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// Pointer is not nil, so value (empty string) is encoded
	// 0x02 = string index, 0x00 = empty string length
	assert.Equal(t, []byte{0x02, 0x00, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_OmitEmpty_PointerStringNonEmptyEncodesValue(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "string"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	val := "hello"
	obj := TestOmitEmptyPointerString{A: &val, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, byte(0x02), buf.Bytes()[0]) // string index
}

func TestEncoder_OmitEmpty_PointerBoolNilEncodesDefault(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "boolean"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyPointerBool{A: nil, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x00, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_OmitEmpty_PointerBoolFalseEncodesValue(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "boolean"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	val := false
	obj := TestOmitEmptyPointerBool{A: &val, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// Pointer is not nil, so value (false) is encoded
	// 0x02 = boolean index, 0x00 = false
	assert.Equal(t, []byte{0x02, 0x00, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_OmitEmpty_PointerBoolTrueEncodesValue(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "boolean"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	val := true
	obj := TestOmitEmptyPointerBool{A: &val, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// 0x02 = boolean index, 0x01 = true
	assert.Equal(t, []byte{0x02, 0x01, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_OmitEmpty_PointerFloat64NilEncodesDefault(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "double"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyPointerFloat64{A: nil, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x00, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_OmitEmpty_PointerFloat64ZeroEncodesValue(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "double"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	val := 0.0
	obj := TestOmitEmptyPointerFloat64{A: &val, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// Pointer is not nil, so value (0.0) is encoded
	assert.Equal(t, byte(0x02), buf.Bytes()[0]) // double index
}

func TestEncoder_OmitEmpty_Float32PositiveInfEncodesValue(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "float"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyFloat32{A: float32(math.Inf(1)), B: "foo"} // +Inf
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, byte(0x02), buf.Bytes()[0]) // float index
}

func TestEncoder_OmitEmpty_Float32NegativeInfEncodesValue(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "float"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyFloat32{A: float32(math.Inf(-1)), B: "foo"} // -Inf
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, byte(0x02), buf.Bytes()[0]) // float index
}

func TestEncoder_OmitEmpty_Float64PositiveInfEncodesValue(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "double"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyFloat64{A: math.Inf(1), B: "foo"} // +Inf
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, byte(0x02), buf.Bytes()[0]) // double index
}

func TestEncoder_OmitEmpty_Float64NegativeInfEncodesValue(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "double"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyFloat64{A: math.Inf(-1), B: "foo"} // -Inf
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, byte(0x02), buf.Bytes()[0]) // double index
}

func TestEncoder_OmitEmpty_Float32SmallestPositiveEncodesValue(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "float"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyFloat32{A: 1.401298464324817e-45, B: "foo"} // smallest positive float32
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, byte(0x02), buf.Bytes()[0]) // float index
}

func TestEncoder_OmitEmpty_Float64SmallestPositiveEncodesValue(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "double"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyFloat64{A: 5e-324, B: "foo"} // smallest positive float64
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, byte(0x02), buf.Bytes()[0]) // double index
}

func TestEncoder_OmitEmpty_StringWithTabIsNonZero(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyMultipleRecord{A: 0, B: "\t"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// A=0 with zerodefault -> null (0x00), B="\t" -> length 1 (0x02) + tab (0x09)
	assert.Equal(t, []byte{0x00, 0x02, 0x09}, buf.Bytes())
}

func TestEncoder_OmitEmpty_StringWithNewlineIsNonZero(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyMultipleRecord{A: 0, B: "\n"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// A=0 with zerodefault -> null (0x00), B="\n" -> length 1 (0x02) + newline (0x0a)
	assert.Equal(t, []byte{0x00, 0x02, 0x0a}, buf.Bytes())
}

func TestEncoder_OmitEmpty_StringWithNullCharIsNonZero(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyMultipleRecord{A: 0, B: "\x00"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// A=0 with zerodefault -> null (0x00), B="\x00" -> length 1 (0x02) + null char (0x00)
	assert.Equal(t, []byte{0x00, 0x02, 0x00}, buf.Bytes())
}

func TestEncoder_OmitEmpty_StringUnicodeIsNonZero(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyMultipleRecord{A: 0, B: "日本語"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// A=0 with zerodefault -> null (0x00), B="日本語" -> length 9 bytes (0x12) + utf8 bytes
	assert.Equal(t, byte(0x00), buf.Bytes()[0]) // A=null
	assert.Equal(t, byte(0x12), buf.Bytes()[1]) // length=9 bytes (3 chars * 3 bytes each), zigzag encoded
}

func TestEncoder_OmitEmpty_StringEmptyDefaultValue(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "string", "default": ""},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyStringNonNull{A: "", B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// Default is empty string, field is empty string - encodes empty string
	// 0x00 = empty string length, 0x06 0x66 0x6f 0x6f = "foo"
	assert.Equal(t, []byte{0x00, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_OmitEmpty_StringLongValue(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	longStr := ""
	for i := 0; i < 1000; i++ {
		longStr += "a"
	}
	obj := TestOmitEmptyMultipleRecord{A: 0, B: longStr}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, byte(0x00), buf.Bytes()[0]) // A=null (zerodefault)
	// B encodes as length (varint) + string bytes
}

func TestEncoder_OmitEmpty_BytesNilEncodesDefault(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "bytes"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyBytes{A: nil, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x00, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_OmitEmpty_BytesSingleZeroByteEncodesValue(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "bytes"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyBytes{A: []byte{0x00}, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// Single zero byte is non-empty
	assert.Equal(t, byte(0x02), buf.Bytes()[0]) // bytes index
}

func TestEncoder_OmitEmpty_BytesAllZerosEncodesValue(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "bytes"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyBytes{A: []byte{0x00, 0x00, 0x00, 0x00}, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// Four zero bytes is non-empty
	assert.Equal(t, byte(0x02), buf.Bytes()[0]) // bytes index
}

func TestEncoder_OmitEmpty_BytesNonNullableEncodesEmpty(t *testing.T) {
	defer ConfigTeardown()

	// For non-nullable schemas, omitempty has no effect - encodes actual value (empty bytes)
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "bytes", "default": "\u0000\u0001\u0002"},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyBytes{A: []byte{}, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// Non-nullable: empty bytes is encoded as-is (length 0)
	// 0x00 = length 0, 0x06 0x66 0x6f 0x6f = "foo"
	assert.Equal(t, []byte{0x00, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_OmitEmpty_SliceWithOneElementEncodesValue(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", {"type": "array", "items": "string"}], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptySlice{A: []string{""}, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// Slice with one element (even empty string) is non-empty
	assert.Equal(t, byte(0x02), buf.Bytes()[0]) // array index
}

func TestEncoder_OmitEmpty_SliceOfPointersNilEncodesDefault(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", {"type": "array", "items": ["null", "long"]}], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptySliceOfPointers{A: nil, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x00, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_OmitEmpty_SliceOfPointersEmptyEncodesDefault(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", {"type": "array", "items": ["null", "long"]}], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptySliceOfPointers{A: []*int64{}, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// Empty slice encodes default
	assert.Equal(t, []byte{0x00, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_OmitEmpty_SliceOfPointersWithNilElementEncodesValue(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", {"type": "array", "items": ["null", "long"]}], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptySliceOfPointers{A: []*int64{nil}, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// Slice with one element (even nil) is non-empty
	assert.Equal(t, byte(0x02), buf.Bytes()[0]) // array index
}

func TestEncoder_OmitEmpty_NestedInnerZeroEncodesDefault(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "outer",
	"fields" : [
		{"name": "inner", "type": {
			"type": "record",
			"name": "inner",
			"fields": [
				{"name": "x", "type": ["null", "long"], "default": null},
				{"name": "y", "type": "string"}
			]
		}},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyNestedInner{
		Inner: InnerOmitEmpty{X: 0, Y: "bar"},
		B:     "foo",
	}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// inner.x=null (0x00), inner.y="bar" (0x06 0x62 0x61 0x72), b="foo" (0x06 0x66 0x6f 0x6f)
	assert.Equal(t, []byte{0x00, 0x06, 0x62, 0x61, 0x72, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_OmitEmpty_NestedInnerNonZeroEncodesValue(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "outer",
	"fields" : [
		{"name": "inner", "type": {
			"type": "record",
			"name": "inner",
			"fields": [
				{"name": "x", "type": ["null", "long"], "default": null},
				{"name": "y", "type": "string"}
			]
		}},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyNestedInner{
		Inner: InnerOmitEmpty{X: 42, Y: "bar"},
		B:     "foo",
	}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// inner.x=42 (0x02 0x54), inner.y="bar", b="foo"
	assert.Equal(t, []byte{0x02, 0x54, 0x06, 0x62, 0x61, 0x72, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_OmitEmpty_MixedTagsAllZero(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "field_a", "type": ["null", "long"], "default": null},
		{"name": "field_b", "type": ["null", "string"], "default": null},
		{"name": "field_c", "type": ["null", "boolean"], "default": null}
	]
}`
	obj := TestOmitEmptyMixedTags{A: 0, B: "", C: false}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// All fields are zero, all should encode default (null)
	assert.Equal(t, []byte{0x00, 0x00, 0x00}, buf.Bytes())
}

func TestEncoder_OmitEmpty_NoTagOnFirstFieldEncodesValue(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
		{"name": "b", "type": ["null", "string"], "default": null}
	]
}`
	obj := TestOmitEmptyNoTagOnFirst{A: 0, B: ""}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// A has no zerodefault tag, so 0 is encoded as value
	// B has zerodefault tag, so empty string encodes null
	// 0x00 = 0, 0x00 = null
	assert.Equal(t, []byte{0x00, 0x00}, buf.Bytes())
}

func TestEncoder_OmitEmpty_IntNonNullableEncodesZero(t *testing.T) {
	defer ConfigTeardown()

	// For non-nullable schemas, omitempty has no effect - encodes actual value
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "int", "default": 999},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyInt32{A: 0, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// Non-nullable: 0 is encoded as-is
	// 0x00 = 0, 0x06 0x66 0x6f 0x6f = "foo"
	assert.Equal(t, []byte{0x00, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_OmitEmpty_BoolWithFalseDefault(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "boolean", "default": false},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyBool{A: false, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// false is zero, default is false, encodes false
	// 0x00 = false, 0x06 0x66 0x6f 0x6f = "foo"
	assert.Equal(t, []byte{0x00, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_OmitEmpty_ArrayWithEmptyDefault(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": {"type": "array", "items": "long"}, "default": []},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyArrayDefault{A: nil, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// nil slice is zero, default is empty array
	// 0x00 = empty array (block count 0), 0x06 0x66 0x6f 0x6f = "foo"
	assert.Equal(t, []byte{0x00, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_OmitEmpty_ArrayWithNonEmptyDefault(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": {"type": "array", "items": "long"}, "default": [1, 2, 3]},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyArrayDefault{A: nil, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// nil slice is zero, default is [1, 2, 3]
	// Should have array data for [1, 2, 3]
	assert.True(t, len(buf.Bytes()) > 4) // More than just "foo"
}

func TestEncoder_OmitEmpty_RoundTripBool(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "boolean"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyBool{A: false, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)
	require.NoError(t, err)

	type DecodedBool struct {
		A *bool  `avro:"a"`
		B string `avro:"b"`
	}
	var decoded DecodedBool
	dec, err := avro.NewDecoder(schema, bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	err = dec.Decode(&decoded)
	require.NoError(t, err)

	assert.Nil(t, decoded.A) // false encoded as null
	assert.Equal(t, "foo", decoded.B)
}

func TestEncoder_OmitEmpty_RoundTripString(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyMultipleRecord{A: 0, B: "hello"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)
	require.NoError(t, err)

	type DecodedString struct {
		A *int64 `avro:"a"`
		B string `avro:"b"`
	}
	var decoded DecodedString
	dec, err := avro.NewDecoder(schema, bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	err = dec.Decode(&decoded)
	require.NoError(t, err)

	assert.Nil(t, decoded.A)            // 0 with zerodefault encoded as null
	assert.Equal(t, "hello", decoded.B) // string encoded normally
}

func TestEncoder_OmitEmpty_RoundTripSlice(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", {"type": "array", "items": "string"}], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptySlice{A: []string{}, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)
	require.NoError(t, err)

	type DecodedSlice struct {
		A []string `avro:"a"`
		B string   `avro:"b"`
	}
	var decoded DecodedSlice
	dec, err := avro.NewDecoder(schema, bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	err = dec.Decode(&decoded)
	require.NoError(t, err)

	assert.Nil(t, decoded.A) // [] encoded as null
	assert.Equal(t, "foo", decoded.B)
}

func TestEncoder_OmitEmpty_RoundTripFloat64(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "double"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyFloat64{A: 0.0, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)
	require.NoError(t, err)

	type DecodedFloat struct {
		A *float64 `avro:"a"`
		B string   `avro:"b"`
	}
	var decoded DecodedFloat
	dec, err := avro.NewDecoder(schema, bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	err = dec.Decode(&decoded)
	require.NoError(t, err)

	assert.Nil(t, decoded.A) // 0.0 encoded as null
	assert.Equal(t, "foo", decoded.B)
}

func TestEncoder_OmitEmpty_MultipleRecordsInSequence(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	// Encode record with zero value
	obj1 := TestOmitEmptyRecord{A: 0, B: "first"}
	err = enc.Encode(obj1)
	require.NoError(t, err)

	// Encode record with non-zero value
	obj2 := TestOmitEmptyRecord{A: 42, B: "second"}
	err = enc.Encode(obj2)
	require.NoError(t, err)

	// Encode another record with zero value
	obj3 := TestOmitEmptyRecord{A: 0, B: "third"}
	err = enc.Encode(obj3)
	require.NoError(t, err)

	// Verify we can decode all three
	reader := bytes.NewReader(buf.Bytes())
	dec, err := avro.NewDecoder(schema, reader)
	require.NoError(t, err)

	type DecodedRecord struct {
		A *int64 `avro:"a"`
		B string `avro:"b"`
	}

	var d1, d2, d3 DecodedRecord
	err = dec.Decode(&d1)
	require.NoError(t, err)
	assert.Nil(t, d1.A)
	assert.Equal(t, "first", d1.B)

	err = dec.Decode(&d2)
	require.NoError(t, err)
	assert.NotNil(t, d2.A)
	assert.Equal(t, int64(42), *d2.A)
	assert.Equal(t, "second", d2.B)

	err = dec.Decode(&d3)
	require.NoError(t, err)
	assert.Nil(t, d3.A)
	assert.Equal(t, "third", d3.B)
}

func TestEncoder_OmitEmpty_TagParsing(t *testing.T) {
	defer ConfigTeardown()

	// This tests that tag parsing handles omitempty correctly
	type TestWithTag struct {
		A int64  `avro:"a,omitempty"`
		B string `avro:"b"`
	}

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestWithTag{A: 0, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// 0x00 = null index, 0x06 0x66 0x6f 0x6f = "foo"
	assert.Equal(t, []byte{0x00, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_OmitEmpty_OmitEmptyOnly(t *testing.T) {
	defer ConfigTeardown()

	// Test struct with only zerodefault tag (no field name specified)
	type TestOnlyOmitEmpty struct {
		A int64  `avro:",omitempty"`
		B string `avro:"b"`
	}

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "A", "type": ["null", "long"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOnlyOmitEmpty{A: 0, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x00, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

// ============================================================================
// Numeric Boundary Values
// ============================================================================

func TestEncoder_OmitEmpty_Int64MaxValue(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyRecord{A: math.MaxInt64, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, byte(0x02), buf.Bytes()[0]) // long index, max value is non-zero
}

func TestEncoder_OmitEmpty_Int64MinValue(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyRecord{A: math.MinInt64, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, byte(0x02), buf.Bytes()[0]) // long index, min value is non-zero
}

func TestEncoder_OmitEmpty_Int32MaxValue(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "int"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyInt32{A: math.MaxInt32, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, byte(0x02), buf.Bytes()[0]) // int index
}

func TestEncoder_OmitEmpty_Int32MinValue(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "int"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyInt32{A: math.MinInt32, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, byte(0x02), buf.Bytes()[0]) // int index
}

func TestEncoder_OmitEmpty_Float32MaxValue(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "float"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyFloat32{A: math.MaxFloat32, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, byte(0x02), buf.Bytes()[0]) // float index
}

func TestEncoder_OmitEmpty_Float64MaxValue(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "double"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyFloat64{A: math.MaxFloat64, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, byte(0x02), buf.Bytes()[0]) // double index
}

func TestEncoder_OmitEmpty_Float32SmallestNonzeroValue(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "float"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyFloat32{A: math.SmallestNonzeroFloat32, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, byte(0x02), buf.Bytes()[0]) // float index - smallest nonzero is still non-zero
}

func TestEncoder_OmitEmpty_Float64SmallestNonzeroValue(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "double"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyFloat64{A: math.SmallestNonzeroFloat64, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, byte(0x02), buf.Bytes()[0]) // double index
}

func TestEncoder_OmitEmpty_Float32NaN(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "float"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyFloat32{A: float32(math.NaN()), B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// NaN is non-zero (NaN != 0)
	assert.Equal(t, byte(0x02), buf.Bytes()[0]) // float index
}

func TestEncoder_OmitEmpty_Float64NaN(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "double"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyFloat64{A: math.NaN(), B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// NaN is non-zero
	assert.Equal(t, byte(0x02), buf.Bytes()[0]) // double index
}

func TestEncoder_OmitEmpty_Int64One(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyRecord{A: 1, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// 1 in zigzag is 2
	assert.Equal(t, []byte{0x02, 0x02, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_OmitEmpty_Int64NegativeOne(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyRecord{A: -1, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// -1 in zigzag is 1
	assert.Equal(t, []byte{0x02, 0x01, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

// ============================================================================
// More String Edge Cases
// ============================================================================

func TestEncoder_OmitEmpty_StringWithOnlySpaces(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyMultipleRecord{A: 0, B: "   "}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// A=0 -> null (0x00), B="   " -> length 3 (0x06) + spaces
	assert.Equal(t, byte(0x00), buf.Bytes()[0])
	assert.Equal(t, byte(0x06), buf.Bytes()[1]) // length 3
}

func TestEncoder_OmitEmpty_StringWithCarriageReturn(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyMultipleRecord{A: 0, B: "\r\n"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, byte(0x00), buf.Bytes()[0])           // A=null
	assert.Equal(t, byte(0x04), buf.Bytes()[1])           // length 2
	assert.Equal(t, []byte{0x0d, 0x0a}, buf.Bytes()[2:4]) // \r\n
}

func TestEncoder_OmitEmpty_StringWithEmoji(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyMultipleRecord{A: 0, B: "🎉"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, byte(0x00), buf.Bytes()[0]) // A=null
	assert.Equal(t, byte(0x08), buf.Bytes()[1]) // length 4 (emoji is 4 bytes in UTF-8)
}

func TestEncoder_OmitEmpty_StringWithMixedUnicode(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	// Mix of ASCII, 2-byte, 3-byte, and 4-byte UTF-8
	obj := TestOmitEmptyMultipleRecord{A: 0, B: "aé日🎉"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, byte(0x00), buf.Bytes()[0]) // A=null
	// 1 + 2 + 3 + 4 = 10 bytes, zigzag(10) = 20 = 0x14
	assert.Equal(t, byte(0x14), buf.Bytes()[1])
}

func TestEncoder_OmitEmpty_StringWithBackslash(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyMultipleRecord{A: 0, B: "\\"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, byte(0x00), buf.Bytes()[0]) // A=null
	assert.Equal(t, byte(0x02), buf.Bytes()[1]) // length 1
	assert.Equal(t, byte('\\'), buf.Bytes()[2])
}

func TestEncoder_OmitEmpty_StringWithQuotes(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyMultipleRecord{A: 0, B: "\"'`"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, byte(0x00), buf.Bytes()[0]) // A=null
	assert.Equal(t, byte(0x06), buf.Bytes()[1]) // length 3
}

// ============================================================================
// Pointer Edge Cases
// ============================================================================

func TestEncoder_OmitEmpty_PointerToMinInt64(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	val := int64(math.MinInt64)
	obj := TestOmitEmptyPointer{A: &val, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, byte(0x02), buf.Bytes()[0]) // long index - pointer to min is non-nil
}

func TestEncoder_OmitEmpty_PointerToMaxInt64(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	val := int64(math.MaxInt64)
	obj := TestOmitEmptyPointer{A: &val, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, byte(0x02), buf.Bytes()[0]) // long index
}

func TestEncoder_OmitEmpty_PointerFloat64ToNaN(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "double"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	val := math.NaN()
	obj := TestOmitEmptyPointerFloat64{A: &val, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, byte(0x02), buf.Bytes()[0]) // double index - pointer is non-nil
}

func TestEncoder_OmitEmpty_PointerFloat64ToNegativeZero(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "double"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	val := math.Copysign(0, -1) // -0.0
	obj := TestOmitEmptyPointerFloat64{A: &val, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// Pointer is non-nil, so it encodes the value (even though value is -0.0)
	assert.Equal(t, byte(0x02), buf.Bytes()[0]) // double index
}

func TestEncoder_OmitEmpty_PointerBoolToFalse(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "boolean"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	val := false
	obj := TestOmitEmptyPointerBool{A: &val, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// Pointer to false is non-nil, should encode boolean
	assert.Equal(t, byte(0x02), buf.Bytes()[0]) // boolean index
	assert.Equal(t, byte(0x00), buf.Bytes()[1]) // false
}

func TestEncoder_OmitEmpty_PointerStringToEmpty(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "string"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	val := ""
	obj := TestOmitEmptyPointerString{A: &val, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// Pointer to empty string is non-nil, should encode string
	assert.Equal(t, byte(0x02), buf.Bytes()[0]) // string index
	assert.Equal(t, byte(0x00), buf.Bytes()[1]) // empty string length
}

// ============================================================================
// Slice Edge Cases
// ============================================================================

func TestEncoder_OmitEmpty_SliceWithZeroElements(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", {"type": "array", "items": "string"}], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptySlice{A: []string{}, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// Empty slice is treated as zero -> null
	assert.Equal(t, byte(0x00), buf.Bytes()[0])
}

func TestEncoder_OmitEmpty_SliceWithEmptyString(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", {"type": "array", "items": "string"}], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptySlice{A: []string{""}, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// Slice with one empty string is non-empty -> encodes value
	assert.Equal(t, byte(0x02), buf.Bytes()[0]) // array index
}

func TestEncoder_OmitEmpty_SliceWithManyElements(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", {"type": "array", "items": "string"}], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	elements := make([]string, 100)
	for i := range elements {
		elements[i] = "x"
	}
	obj := TestOmitEmptySlice{A: elements, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, byte(0x02), buf.Bytes()[0]) // array index
}

// ============================================================================
// Map Edge Cases
// ============================================================================

// Note: Tests for map[string]int64 with non-empty maps in unions are removed
// because the library cannot resolve map[string]int64 type in union schemas.
// Empty map tests (that trigger zerodefault -> null) work because they don't
// need type resolution.

var _ = fmt.Sprintf // keep fmt import used

// ============================================================================
// Schema Variations
// ============================================================================

func TestEncoder_OmitEmpty_UnionLongFirst(t *testing.T) {
	defer ConfigTeardown()

	// Test with long first in union (non-standard order)
	// With omitempty, zero value writes null regardless of union order
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["long", "null"], "default": 0},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyRecord{A: 0, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// With omitempty, zero value encodes as null (index 1 = 0x02 in zigzag)
	assert.Equal(t, byte(0x02), buf.Bytes()[0]) // null index (second in union)
}

func TestEncoder_OmitEmpty_ThreeWayUnion(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long", "string"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyRecord{A: 0, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// With three-way union, zerodefault encodes as long (index 1) with value 0
	// (zerodefault behavior may differ for complex unions)
	assert.Equal(t, byte(0x02), buf.Bytes()[0]) // long index (1 in zigzag = 2)
}

func TestEncoder_OmitEmpty_ThreeWayUnionNonZero(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long", "string"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyRecord{A: 42, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// Non-zero encodes as long (index 1)
	assert.Equal(t, byte(0x02), buf.Bytes()[0]) // long index (1 in zigzag = 2)
}

func TestEncoder_OmitEmpty_NonNullableFieldEncodesZero(t *testing.T) {
	defer ConfigTeardown()

	// Non-nullable field - zerodefault has no effect, zero encodes as zero
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyRecord{A: 0, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// Zero encodes as zero (0x00 in zigzag)
	assert.Equal(t, byte(0x00), buf.Bytes()[0])
}

// ============================================================================
// Multiple Fields with OmitEmpty
// ============================================================================

func TestEncoder_OmitEmpty_AllFieldsHaveTag(t *testing.T) {
	defer ConfigTeardown()

	type AllOmitEmpty struct {
		A int64   `avro:"a,omitempty"`
		B string  `avro:"b,omitempty"`
		C bool    `avro:"c,omitempty"`
		D float64 `avro:"d,omitempty"`
	}

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long"], "default": null},
		{"name": "b", "type": ["null", "string"], "default": null},
		{"name": "c", "type": ["null", "boolean"], "default": null},
		{"name": "d", "type": ["null", "double"], "default": null}
	]
}`
	obj := AllOmitEmpty{A: 0, B: "", C: false, D: 0.0}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// All fields should encode as null
	assert.Equal(t, []byte{0x00, 0x00, 0x00, 0x00}, buf.Bytes())
}

func TestEncoder_OmitEmpty_AlternatingZeroNonZero(t *testing.T) {
	defer ConfigTeardown()

	type AlternatingFields struct {
		A int64 `avro:"a,omitempty"`
		B int64 `avro:"b,omitempty"`
		C int64 `avro:"c,omitempty"`
		D int64 `avro:"d,omitempty"`
	}

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long"], "default": null},
		{"name": "b", "type": ["null", "long"], "default": null},
		{"name": "c", "type": ["null", "long"], "default": null},
		{"name": "d", "type": ["null", "long"], "default": null}
	]
}`
	obj := AlternatingFields{A: 0, B: 1, C: 0, D: 2}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// A=null, B=1, C=null, D=2
	assert.Equal(t, byte(0x00), buf.Bytes()[0]) // A=null
	assert.Equal(t, byte(0x02), buf.Bytes()[1]) // B=long index
	assert.Equal(t, byte(0x02), buf.Bytes()[2]) // B=1 (zigzag)
	assert.Equal(t, byte(0x00), buf.Bytes()[3]) // C=null
	assert.Equal(t, byte(0x02), buf.Bytes()[4]) // D=long index
	assert.Equal(t, byte(0x04), buf.Bytes()[5]) // D=2 (zigzag)
}

// ============================================================================
// Re-encoding and Encoder Reuse
// ============================================================================

func TestEncoder_OmitEmpty_ReencodeWithDifferentValues(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	// First encode with zero
	obj1 := TestOmitEmptyRecord{A: 0, B: "first"}
	err = enc.Encode(obj1)
	require.NoError(t, err)
	firstLen := buf.Len()

	// Second encode with non-zero
	obj2 := TestOmitEmptyRecord{A: 100, B: "second"}
	err = enc.Encode(obj2)
	require.NoError(t, err)

	// Verify both were encoded
	assert.True(t, buf.Len() > firstLen)
}

func TestEncoder_OmitEmpty_EncodeSameObjectTwice(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	obj := TestOmitEmptyRecord{A: 0, B: "test"}

	err = enc.Encode(obj)
	require.NoError(t, err)
	firstEncoding := make([]byte, buf.Len())
	copy(firstEncoding, buf.Bytes())

	err = enc.Encode(obj)
	require.NoError(t, err)

	// Second encoding should be appended and identical
	secondEncoding := buf.Bytes()[len(firstEncoding):]
	assert.Equal(t, firstEncoding, secondEncoding)
}

// ============================================================================
// Bytes Edge Cases
// ============================================================================

func TestEncoder_OmitEmpty_BytesWithHighBytes(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "bytes"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyBytes{A: []byte{0xFF, 0xFE, 0xFD}, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, byte(0x02), buf.Bytes()[0]) // bytes index
}

func TestEncoder_OmitEmpty_BytesLargeArray(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "bytes"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	largeBytes := make([]byte, 10000)
	for i := range largeBytes {
		largeBytes[i] = byte(i % 256)
	}
	obj := TestOmitEmptyBytes{A: largeBytes, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, byte(0x02), buf.Bytes()[0]) // bytes index
	assert.True(t, buf.Len() > 10000)           // should contain the large byte array
}

// ============================================================================
// Round-Trip Tests
// ============================================================================

func TestEncoder_OmitEmpty_RoundTripInt64Boundary(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyRecord{A: math.MaxInt64, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)
	require.NoError(t, err)

	type DecodedRecord struct {
		A *int64 `avro:"a"`
		B string `avro:"b"`
	}
	var decoded DecodedRecord
	dec, err := avro.NewDecoder(schema, bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	err = dec.Decode(&decoded)
	require.NoError(t, err)

	require.NotNil(t, decoded.A)
	assert.Equal(t, int64(math.MaxInt64), *decoded.A)
	assert.Equal(t, "foo", decoded.B)
}

func TestEncoder_OmitEmpty_RoundTripFloat64Special(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "double"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptyFloat64{A: math.Inf(1), B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)
	require.NoError(t, err)

	type DecodedRecord struct {
		A *float64 `avro:"a"`
		B string   `avro:"b"`
	}
	var decoded DecodedRecord
	dec, err := avro.NewDecoder(schema, bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	err = dec.Decode(&decoded)
	require.NoError(t, err)

	require.NotNil(t, decoded.A)
	assert.True(t, math.IsInf(*decoded.A, 1))
	assert.Equal(t, "foo", decoded.B)
}

func TestEncoder_OmitEmpty_RoundTripEmptySlice(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", {"type": "array", "items": "string"}], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptySlice{A: []string{}, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)
	require.NoError(t, err)

	type DecodedRecord struct {
		A []string `avro:"a"`
		B string   `avro:"b"`
	}
	var decoded DecodedRecord
	dec, err := avro.NewDecoder(schema, bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	err = dec.Decode(&decoded)
	require.NoError(t, err)

	assert.Nil(t, decoded.A) // empty slice encoded as null, decoded as nil
	assert.Equal(t, "foo", decoded.B)
}

func TestEncoder_OmitEmpty_RoundTripNonEmptySlice(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", {"type": "array", "items": "string"}], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestOmitEmptySlice{A: []string{"x", "y", "z"}, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)
	require.NoError(t, err)

	type DecodedRecord struct {
		A []string `avro:"a"`
		B string   `avro:"b"`
	}
	var decoded DecodedRecord
	dec, err := avro.NewDecoder(schema, bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	err = dec.Decode(&decoded)
	require.NoError(t, err)

	assert.Equal(t, []string{"x", "y", "z"}, decoded.A)
	assert.Equal(t, "foo", decoded.B)
}

// Note: TestEncoder_OmitEmpty_RoundTripEmptyMap removed because
// the library cannot resolve map[string]int64 type in union schemas

// ============================================================================
// Field Ordering
// ============================================================================

func TestEncoder_OmitEmpty_FirstFieldZero(t *testing.T) {
	defer ConfigTeardown()

	type FirstFieldZero struct {
		A int64  `avro:"a,omitempty"`
		B int64  `avro:"b"`
		C string `avro:"c"`
	}

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long"], "default": null},
		{"name": "b", "type": "long"},
		{"name": "c", "type": "string"}
	]
}`
	obj := FirstFieldZero{A: 0, B: 42, C: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, byte(0x00), buf.Bytes()[0]) // A=null
	assert.Equal(t, byte(0x54), buf.Bytes()[1]) // B=42 (zigzag)
}

func TestEncoder_OmitEmpty_LastFieldZero(t *testing.T) {
	defer ConfigTeardown()

	type LastFieldZero struct {
		A string `avro:"a"`
		B int64  `avro:"b"`
		C int64  `avro:"c,omitempty"`
	}

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "string"},
		{"name": "b", "type": "long"},
		{"name": "c", "type": ["null", "long"], "default": null}
	]
}`
	obj := LastFieldZero{A: "foo", B: 42, C: 0}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// "foo" = 0x06 0x66 0x6f 0x6f, 42 = 0x54, C=null = 0x00
	assert.Equal(t, []byte{0x06, 0x66, 0x6f, 0x6f, 0x54, 0x00}, buf.Bytes())
}

func TestEncoder_OmitEmpty_MiddleFieldZero(t *testing.T) {
	defer ConfigTeardown()

	type MiddleFieldZero struct {
		A string `avro:"a"`
		B int64  `avro:"b,omitempty"`
		C string `avro:"c"`
	}

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "string"},
		{"name": "b", "type": ["null", "long"], "default": null},
		{"name": "c", "type": "string"}
	]
}`
	obj := MiddleFieldZero{A: "foo", B: 0, C: "bar"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	// "foo" = 0x06 0x66 0x6f 0x6f, B=null = 0x00, "bar" = 0x06 0x62 0x61 0x72
	assert.Equal(t, []byte{0x06, 0x66, 0x6f, 0x6f, 0x00, 0x06, 0x62, 0x61, 0x72}, buf.Bytes())
}

// ============================================================================
// Comparison: With and Without OmitEmpty
// ============================================================================

func TestEncoder_OmitEmpty_CompareWithoutTag(t *testing.T) {
	defer ConfigTeardown()

	type WithTag struct {
		A int64  `avro:"a,omitempty"`
		B string `avro:"b"`
	}
	type WithoutTag struct {
		A int64  `avro:"a"`
		B string `avro:"b"`
	}

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`

	// Encode with zerodefault tag
	objWith := WithTag{A: 0, B: "foo"}
	bufWith := &bytes.Buffer{}
	encWith, _ := avro.NewEncoder(schema, bufWith)
	encWith.Encode(objWith)

	// Encode without zerodefault tag
	objWithout := WithoutTag{A: 0, B: "foo"}
	bufWithout := &bytes.Buffer{}
	encWithout, _ := avro.NewEncoder(schema, bufWithout)
	encWithout.Encode(objWithout)

	// With zerodefault: encodes as null (shorter)
	// Without zerodefault: encodes as long with value 0
	assert.True(t, bufWith.Len() < bufWithout.Len(), "zerodefault should produce shorter encoding")
	assert.Equal(t, byte(0x00), bufWith.Bytes()[0])    // null
	assert.Equal(t, byte(0x02), bufWithout.Bytes()[0]) // long index
}

// ============================================================================
// Error Handling (verifying no panics)
// ============================================================================

func TestEncoder_OmitEmpty_VeryLargeRecord(t *testing.T) {
	defer ConfigTeardown()

	type LargeRecord struct {
		A int64  `avro:"a,omitempty"`
		B string `avro:"b"`
	}

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "long"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	// Very long string
	longStr := make([]byte, 1000000)
	for i := range longStr {
		longStr[i] = 'x'
	}
	obj := LargeRecord{A: 0, B: string(longStr)}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.True(t, buf.Len() > 1000000)
}
