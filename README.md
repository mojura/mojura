# Service core
Service core is a service helper library for core ancillary methods

## Usage

### New
```go
func ExampleNew() {
	var (
		c   *Core
		err error
	)

	if c, err = New("example", "./data", testStruct{}, "users", "contacts"); err != nil {
		return
	}

	fmt.Printf("Core! %v\n", c)
}
```

### Core.New
```go
func ExampleCore_New() {
	var ts testStruct
	ts.Foo = "Foo foo"
	ts.Bar = "Bar bar"

	var (
		entryID string
		err     error
	)

	if entryID, err = c.New(&ts, "user_1", "contact_3"); err != nil {
		return
	}

	fmt.Printf("New entry! %s\n", entryID)
}
```

### Core.Get
```go
func ExampleCore_Get() {
	var (
		ts  testStruct
		err error
	)

	if err = c.Get("00000000", &ts); err != nil {
		return
	}

	fmt.Printf("Retrieved entry! %+v\n", ts)
}
```

### Core.GetByRelationship
```go
func ExampleCore_GetByRelationship() {
	var (
		tss []*testStruct
		err error
	)

	if err = c.GetByRelationship("users", "user_1", &tss); err != nil {
		return
	}

	for i, ts := range tss {
		fmt.Printf("Retrieved entry #%d! %+v\n", i, ts)
	}
}
```

### Core.Remove
```go
func ExampleCore_Remove() {
	var err error
	if err = c.Remove("00000000"); err != nil {
		return
	}

	fmt.Printf("Removed entry %s!\n", "00000000")
}
```
