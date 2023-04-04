package mojura

import "reflect"

func makeType[T any]() func() T {
	var ref T
	typ := reflect.TypeOf(ref)
	kind := typ.Kind()
	if kind != reflect.Ptr {
		return makeWithoutReflect[T]
	}

	return makeWithReflect[T](typ, kind)
}

func makeWithoutReflect[T any]() (out T) {
	return
}

func makeWithReflect[T any](typ reflect.Type, kind reflect.Kind) func() T {
	types := getTypes(typ, kind)
	return func() (out T) {
		val := reflect.ValueOf(&out).Elem()
		for _, typ := range types {
			created := reflect.New(typ)
			val.Set(created)
			val = val.Elem()
		}

		return
	}
}

func getTypes(typ reflect.Type, kind reflect.Kind) (types []reflect.Type) {
	for kind == reflect.Ptr {
		typ = typ.Elem()
		kind = typ.Kind()
		types = append(types, typ)

	}

	return
}
