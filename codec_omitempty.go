package avro

import (
	"reflect"
	"unsafe"

	"github.com/modern-go/reflect2"
)

// isEmptyFunc returns an efficient zero-check function for the given type.
// This matches the behavior of encoding/json's omitempty and is similar to
// jsoniter's IsEmpty implementation: https://github.com/json-iterator/go/blob/master/reflect_native.go
// For struct/array types, returns a function that always returns false.
func isEmptyFunc(typ reflect2.Type) func(unsafe.Pointer) bool {
	switch typ.Kind() {
	case reflect.Bool:
		return func(ptr unsafe.Pointer) bool { return !*(*bool)(ptr) }
	case reflect.Int:
		return func(ptr unsafe.Pointer) bool { return *(*int)(ptr) == 0 }
	case reflect.Int8:
		return func(ptr unsafe.Pointer) bool { return *(*int8)(ptr) == 0 }
	case reflect.Int16:
		return func(ptr unsafe.Pointer) bool { return *(*int16)(ptr) == 0 }
	case reflect.Int32:
		return func(ptr unsafe.Pointer) bool { return *(*int32)(ptr) == 0 }
	case reflect.Int64:
		return func(ptr unsafe.Pointer) bool { return *(*int64)(ptr) == 0 }
	case reflect.Uint:
		return func(ptr unsafe.Pointer) bool { return *(*uint)(ptr) == 0 }
	case reflect.Uint8:
		return func(ptr unsafe.Pointer) bool { return *(*uint8)(ptr) == 0 }
	case reflect.Uint16:
		return func(ptr unsafe.Pointer) bool { return *(*uint16)(ptr) == 0 }
	case reflect.Uint32:
		return func(ptr unsafe.Pointer) bool { return *(*uint32)(ptr) == 0 }
	case reflect.Uint64:
		return func(ptr unsafe.Pointer) bool { return *(*uint64)(ptr) == 0 }
	case reflect.Float32:
		return func(ptr unsafe.Pointer) bool { return *(*float32)(ptr) == 0 }
	case reflect.Float64:
		return func(ptr unsafe.Pointer) bool { return *(*float64)(ptr) == 0 }
	case reflect.String:
		return func(ptr unsafe.Pointer) bool { return *(*string)(ptr) == "" }
	case reflect.Slice:
		sliceType := typ.(*reflect2.UnsafeSliceType)
		return func(ptr unsafe.Pointer) bool { return sliceType.UnsafeLengthOf(ptr) == 0 }
	case reflect.Map:
		mapType := typ.(*reflect2.UnsafeMapType)
		return func(ptr unsafe.Pointer) bool {
			if mapType.UnsafeIsNil(ptr) {
				return true
			}
			// Check if map is empty by iterating
			iter := mapType.UnsafeIterate(ptr)
			return !iter.HasNext()
		}
	case reflect.Ptr:
		ptrType := typ.(*reflect2.UnsafePtrType)
		return func(ptr unsafe.Pointer) bool { return ptrType.UnsafeIsNil(ptr) }
	case reflect.Interface:
		return func(ptr unsafe.Pointer) bool { return *(*unsafe.Pointer)(ptr) == nil }
	default:
		// Struct, Array, and other complex types are never considered empty
		return func(ptr unsafe.Pointer) bool { return false }
	}
}
