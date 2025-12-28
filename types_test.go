package avro_test

import "errors"

type TestInterface interface {
	SomeFunc() int
}

type TestRecord struct {
	A int64  `avro:"a"`
	B string `avro:"b"`
}

func (*TestRecord) SomeFunc() int {
	return 0
}

type UnionRecordParent struct {
	UnionRecord *UnionRecord `avro:"union"`
}

type UnionRecord struct {
	Int  *int
	Test *TestRecord
}

func (u *UnionRecord) ToAny() (any, error) {
	if u.Int != nil {
		return u.Int, nil
	} else if u.Test != nil {
		return u.Test, nil
	}

	return nil, errors.New("no value to encode")
}

func (u *UnionRecord) FromAny(payload any) error {
	switch t := payload.(type) {
	case int:
		u.Int = &t
	case TestRecord:
		u.Test = &t
	default:
		return errors.New("unknown type during decode of union")
	}

	return nil
}

type TestPartialRecord struct {
	B string `avro:"b"`
}

type TestNestedRecord struct {
	A TestRecord `avro:"a"`
	B TestRecord `avro:"b"`
}

type TestUnion struct {
	A any `avro:"a"`
}

type TestEmbeddedRecord struct {
	C string `avro:"c"`

	TestEmbed // tests not-first position
}

type TestEmbeddedPtrRecord struct {
	C string `avro:"c"`

	*TestEmbed // tests not-first position
}

type TestEmbed struct {
	A int64  `avro:"a"`
	B string `avro:"b"`
}

type TestEmbedInt int

type TestEmbeddedIntRecord struct {
	B string `avro:"b"`

	TestEmbedInt // tests not-first position
}

type TestUnexportedRecord struct {
	A int64  `avro:"a"`
	b string `avro:"b"`
}

type TestOmitEmptyRecord struct {
	A int64  `avro:"a,omitempty"`
	B string `avro:"b"`
}

type TestOmitEmptyMultipleRecord struct {
	A int64  `avro:"a,omitempty"`
	B string `avro:"b,omitempty"`
}

type TestOmitEmptyNoDefault struct {
	A int64  `avro:"a,omitempty"`
	B string `avro:"b"`
}

// Additional types for OmitEmpty edge case testing

type TestOmitEmptyBool struct {
	A bool   `avro:"a,omitempty"`
	B string `avro:"b"`
}

type TestOmitEmptyFloat32 struct {
	A float32 `avro:"a,omitempty"`
	B string  `avro:"b"`
}

type TestOmitEmptyFloat64 struct {
	A float64 `avro:"a,omitempty"`
	B string  `avro:"b"`
}

type TestOmitEmptyInt32 struct {
	A int32  `avro:"a,omitempty"`
	B string `avro:"b"`
}

type TestOmitEmptySlice struct {
	A []string `avro:"a,omitempty"`
	B string   `avro:"b"`
}

type TestOmitEmptyMap struct {
	A map[string]any `avro:"a,omitempty"`
	B string         `avro:"b"`
}

type TestOmitEmptyPointer struct {
	A *int64 `avro:"a,omitempty"`
	B string `avro:"b"`
}

type TestOmitEmptyBytes struct {
	A []byte `avro:"a,omitempty"`
	B string `avro:"b"`
}

type TestOmitEmptyStringNonNull struct {
	A string `avro:"a,omitempty"`
	B string `avro:"b"`
}

type TestOmitEmptyAllFields struct {
	A int64  `avro:"a,omitempty"`
	B string `avro:"b,omitempty"`
	C bool   `avro:"c,omitempty"`
}

type TestOmitEmptyNested struct {
	Inner TestOmitEmptyRecord `avro:"inner"`
	B     string              `avro:"b"`
}

type TestOmitEmptyEmbedded struct {
	TestOmitEmptyRecord
	C string `avro:"c"`
}

// Additional types for comprehensive OmitEmpty edge case testing

type TestOmitEmptyUint struct {
	A uint   `avro:"a,omitempty"`
	B string `avro:"b"`
}

type TestOmitEmptyUint8 struct {
	A uint8  `avro:"a,omitempty"`
	B string `avro:"b"`
}

type TestOmitEmptyUint16 struct {
	A uint16 `avro:"a,omitempty"`
	B string `avro:"b"`
}

type TestOmitEmptyUint32 struct {
	A uint32 `avro:"a,omitempty"`
	B string `avro:"b"`
}

type TestOmitEmptyUint64 struct {
	A uint64 `avro:"a,omitempty"`
	B string `avro:"b"`
}

type TestOmitEmptyInt struct {
	A int    `avro:"a,omitempty"`
	B string `avro:"b"`
}

type TestOmitEmptyInt8 struct {
	A int8   `avro:"a,omitempty"`
	B string `avro:"b"`
}

type TestOmitEmptyInt16 struct {
	A int16  `avro:"a,omitempty"`
	B string `avro:"b"`
}

type MyCustomInt int64

type TestOmitEmptyCustomInt struct {
	A MyCustomInt `avro:"a,omitempty"`
	B string      `avro:"b"`
}

type MyCustomString string

type TestOmitEmptyCustomString struct {
	A MyCustomString `avro:"a,omitempty"`
	B string         `avro:"b"`
}

type TestOmitEmptyPointerToStruct struct {
	A *TestRecord `avro:"a,omitempty"`
	B string      `avro:"b"`
}

type TestOmitEmptySliceOfPointers struct {
	A []*int64 `avro:"a,omitempty"`
	B string   `avro:"b"`
}

type TestOmitEmptyMixedTags struct {
	A int64  `avro:"field_a,omitempty,omitempty"`
	B string `avro:"field_b,omitempty"`
	C bool   `avro:"field_c,omitempty"`
}

type TestOmitEmptyNoTagOnFirst struct {
	A int64  `avro:"a"`
	B string `avro:"b,omitempty"`
}

type InnerOmitEmpty struct {
	X int64  `avro:"x,omitempty"`
	Y string `avro:"y"`
}

type TestOmitEmptyNestedInner struct {
	Inner InnerOmitEmpty `avro:"inner"`
	B     string         `avro:"b"`
}

type TestOmitEmptyPointerString struct {
	A *string `avro:"a,omitempty"`
	B string  `avro:"b"`
}

type TestOmitEmptyPointerBool struct {
	A *bool  `avro:"a,omitempty"`
	B string `avro:"b"`
}

type TestOmitEmptyPointerFloat64 struct {
	A *float64 `avro:"a,omitempty"`
	B string   `avro:"b"`
}

type TestOmitEmptyArrayDefault struct {
	A []int64 `avro:"a,omitempty"`
	B string  `avro:"b"`
}

type TestOmitEmptyMapStringInt struct {
	A map[string]int64 `avro:"a,omitempty"`
	B string           `avro:"b"`
}
