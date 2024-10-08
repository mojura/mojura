# Mojura
<!-- ALL-CONTRIBUTORS-BADGE:START - Do not remove or modify this section -->
[![All Contributors](https://img.shields.io/badge/all_contributors-5-orange.svg?style=flat-square)](#contributors-)
<!-- ALL-CONTRIBUTORS-BADGE:END -->
Mojura is a filter-based programmatic relational DB which leverages any K/V store as it's backend.

![billboard](https://github.com/mojura/mojura/blob/main/mojura-billboard.png?raw=true "Mojura billboard")

## Usage
For a step-by-step usage guide, see our [example repository](https://github.com/mojura/hello-world/wiki)!

### New
```go
func ExampleNew() {
	var (
		c   *Mojura[*testStruct]
		err error
	)

	opts := MakeOpts("example", "./data")

	if c, err = New[*testStruct](opts, "users", "contacts", "groups"); err != nil {
		return
	}

	fmt.Printf("Mojura! %v\n", c)
}
```

### Mojura.New
```go
func ExampleMojura_New() {
	var ts testStruct
	ts.UserID = "user_1"
	ts.Value = "Foo bar"

	var (
		created *testStruct
		err     error
	)

	if created, err = c.New(&ts); err != nil {
		return
	}

	fmt.Printf("New entry! %+v\n", created)
}
```

### Mojura.Get
```go
func ExampleMojura_Get() {
	var (
		ts  *testStruct
		err error
	)

	if ts, err = c.Get("00000000"); err != nil {
		return
	}

	fmt.Printf("Retrieved entry! %+v\n", ts)
}
```

### Mojura.GetFiltered
```go
func ExampleMojura_GetFiltered() {
	var (
		tss    []*testStruct
		lastID string
		err    error
	)

	filter := filters.Match("users", "user_1")
	opts := NewFilteringOpts(filter)

	if tss, lastID, err = c.GetFiltered(opts); err != nil {
		return
	}

	fmt.Printf("Retrieved entries! %+v with a lastID of <%s>\n", tss, lastID)
}
```

### Mojura.ForEach (no filters)
```go
func ExampleMojura_ForEach() {
	var err error
	opts := NewFilteringOpts()
	if err = c.ForEach(func(entryID string, val *testStruct) (err error) {
		fmt.Printf("Iterating entry (%s)! %+v\n", entryID, val)
		return
	}, opts); err != nil {
		return
	}
}
```

### Mojura.ForEach (with filter)
```go
func ExampleMojura_ForEach_with_filter() {
	var err error
	filter := filters.Match("users", "user_1")
	opts := NewFilteringOpts(filter)
	if err = c.ForEach(func(entryID string, val *testStruct) (err error) {
		fmt.Printf("Iterating entry (%s)! %+v\n", entryID, val)
		return
	}, opts); err != nil {
		return
	}
}
```

### Mojura.Update
```go
func ExampleMojura_Update() {
	var err error
	var updated *testStruct
	if updated, err = c.Update("00000000", func(ts *testStruct) (err error) {
		// Let's update the Value field to "New foo value"
		ts.Value = "New foo value"
		return
	}); err != nil {
		return
	}

	fmt.Printf("Edited entry %+v!\n", updated)
}
```

### Mojura.Put
```go
func ExampleMojura_Put() {
	var (
		ts  testStruct
		err error
	)

	// We will pretend the test struct is already populated

	// Let's update the Value field to "New foo value"
	ts.Value = "New foo value"

	var updated *testStruct
	if updated, err = c.Put("00000000", &ts); err != nil {
		return
	}

	fmt.Printf("Updated entry %+v!\n", updated)
}
```

### Mojura.Delete
```go
func ExampleMojura_Delete() {
	var (
		removed *testStruct
		err     error
	)

	if removed, err = c.Delete("00000000"); err != nil {
		return
	}

	fmt.Printf("Removed entry %+v!\n", removed)
}
```

## Contributors ✨

Thanks goes to these wonderful people ([emoji key](https://allcontributors.org/docs/en/emoji-key)):

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->
<table>
  <tr>
    <td align="center"><a href="http://itsmontoya.com"><img src="https://avatars2.githubusercontent.com/u/928954?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Josh</b></sub></a><br /><a href="https://github.com/mojura/mojura/commits?author=itsmontoya" title="Code">💻</a> <a href="https://github.com/mojura/mojura/commits?author=itsmontoya" title="Documentation">📖</a></td>
    <td align="center"><a href="https://github.com/dhalman"><img src="https://avatars3.githubusercontent.com/u/1349742?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Derek Halman</b></sub></a><br /><a href="https://github.com/mojura/mojura/commits?author=dhalman" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/russiansmack"><img src="https://avatars2.githubusercontent.com/u/5841757?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Sergey Anufrienko</b></sub></a><br /><a href="https://github.com/mojura/mojura/commits?author=russiansmack" title="Code">💻</a></td>
    <td align="center"><a href="http://mattstay.com"><img src="https://avatars0.githubusercontent.com/u/414740?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Matt Stay</b></sub></a><br /><a href="#design-matthew-stay" title="Design">🎨</a></td>
    <td align="center"><a href="https://github.com/BrandenWilliams"><img src="https://avatars.githubusercontent.com/u/32830332?v=4?s=100" width="100px;" alt=""/><br /><sub><b>BrandenWilliams</b></sub></a><br /><a href="https://github.com/mojura/mojura/pulls?q=is%3Apr+reviewed-by%3ABrandenWilliams" title="Reviewed Pull Requests">👀</a></td>
  </tr>
</table>

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->

This project follows the [all-contributors](https://github.com/all-contributors/all-contributors) specification. Contributions of any kind welcome!