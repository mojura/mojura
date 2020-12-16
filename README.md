# Mojura
<!-- ALL-CONTRIBUTORS-BADGE:START - Do not remove or modify this section -->
[![All Contributors](https://img.shields.io/badge/all_contributors-3-orange.svg?style=flat-square)](#contributors-)
<!-- ALL-CONTRIBUTORS-BADGE:END -->
Mojura is a service helper library for DB ancillary methods

## Usage

### New
```go
func ExampleNew() {
	var (
		c   *Mojura
		err error
	)

	if c, err = New("example", "./data", testStruct{}, "users", "contacts"); err != nil {
		return
	}

	fmt.Printf("Mojura! %v\n", c)
}
```

### Mojura.New
```go
func ExampleMojura_New() {
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

### Mojura.Get
```go
func ExampleMojura_Get() {
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

### Mojura.GetByRelationship
```go
func ExampleMojura_GetByRelationship() {
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

### Mojura.ForEach
```go

func ExampleMojura_ForEach() {
	var err error
	if err = c.ForEach(func(key string, val Value) (err error) {
		fmt.Printf("Iterating entry (%s)! %+v\n", key, val)
		return
	}); err != nil {
		return
	}
}
```

### Mojura.ForEachRelationship
```go
func ExampleMojura_ForEachRelationship() {
	var err error
	if err = c.ForEachRelationship("users", "user_1", func(key string, val Value) (err error) {
		fmt.Printf("Iterating entry (%s)! %+v\n", key, val)
		return
	}); err != nil {
		return
	}
}
```

### Mojura.Edit
```go
func ExampleMojura_Edit() {
	var err error
	if err = c.Edit("00000000", func(v interface{}) (err error) {
		ts := v.(*testStruct)
		ts.Foo = "New foo value"
		return
	}); err != nil {
		return
	}

	fmt.Printf("Edited entry %s!\n", "00000000")
}
```

### Mojura.Remove
```go
func ExampleMojura_Remove() {
	var err error
	if err = c.Remove("00000000"); err != nil {
		return
	}

	fmt.Printf("Removed entry %s!\n", "00000000")
}
```

## Contributors ✨

Thanks goes to these wonderful people ([emoji key](https://allcontributors.org/docs/en/emoji-key)):

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->
<table>
  <tr>
    <td align="center"><a href="http://itsmontoya.com"><img src="https://avatars2.githubusercontent.com/u/928954?v=4" width="100px;" alt=""/><br /><sub><b>Josh</b></sub></a><br /><a href="https://github.com/mojura/mojura/commits?author=itsmontoya" title="Code">💻</a> <a href="https://github.com/mojura/mojura/commits?author=itsmontoya" title="Documentation">📖</a></td>
    <td align="center"><a href="https://github.com/dhalman"><img src="https://avatars3.githubusercontent.com/u/1349742?v=4" width="100px;" alt=""/><br /><sub><b>Derek Halman</b></sub></a><br /><a href="https://github.com/mojura/mojura/commits?author=dhalman" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/russiansmack"><img src="https://avatars2.githubusercontent.com/u/5841757?v=4" width="100px;" alt=""/><br /><sub><b>Sergey Anufrienko</b></sub></a><br /><a href="https://github.com/mojura/mojura/commits?author=russiansmack" title="Code">💻</a></td>
  </tr>
</table>

<!-- markdownlint-enable -->
<!-- prettier-ignore-end -->
<!-- ALL-CONTRIBUTORS-LIST:END -->

This project follows the [all-contributors](https://github.com/all-contributors/all-contributors) specification. Contributions of any kind welcome!