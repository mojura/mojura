# Go Style Guide

This guide defines the coding standards for Mojura. Build small, simple units with
one clear responsibility. Favor explicit control flow and predictable structure so
the next person can review and maintain the code confidently.

Read [AGENTS.md](AGENTS.md) for the agent workflow and
[docs/development.md](docs/development.md) for architecture and verification.
This file defines style; executable code defines current runtime behavior.

## Unit Philosophy: Small and Simple

**Keep each unit as small and simple as its responsibility allows.** A unit can be
a function, type, file, or package. Its purpose, inputs, outputs, dependencies, and
side effects should be easy to understand on their own. This is a core design
principle for production code, tests, and examples.

* Give each function one job. A function either performs a focused operation or
  coordinates a short sequence of named operations. Keep those levels separate.
* Give each type a cohesive set of state and behavior. Split responsibilities when
  unrelated concerns accumulate; introduce a type only when there is state or an
  invariant for it to own.
* Pass only the values and dependencies a unit needs. Keep data flow explicit,
  minimize mutable state, and return results and errors directly.
* Extract meaningful steps when a function mixes setup, business operations,
  persistence, or presentation. Keep resource acquisition and cleanup together in
  the smallest scope that owns their lifetime.
* Prefer ordinary functions, concrete types, and straightforward control flow.
  Introduce interfaces, generic helpers, and other abstractions when they solve a
  present problem. A little repetition is acceptable when sharing code would make
  it harder to follow.
* Keep related units easy to find. Each helper should name a complete operation and
  reduce what its caller must understand. Avoid chains of trivial forwarding
  functions or extra files that add navigation without clarifying responsibility.

Judge simplicity by the number of responsibilities, branches, dependencies, and
pieces of state a reader must track. Shortening names, compressing statements, or
moving arbitrary lines into helpers does not make a unit simpler. Apply these
principles within the adoption scope below; keep unrelated refactors separate.

## Scope and adoption

Apply this guide to new code and functions being substantially changed. A comment
edit or a one-line bug fix does not require restyling an entire function or file.
Keep broader style migrations in separate, intentional changes.

Existing code is not uniformly compliant, particularly around naked returns and
error wrapping. Its presence is not an exception for new code. Preserve public
APIs, error contracts, and behavior when improving style: changing error identity,
cleanup, or initialization order is a behavior change that needs its own review.

Rules stated as **must**, **do not**, or **never** are requirements within this
scope. **Prefer** identifies a default to apply with judgment. The narrow exceptions
below are part of the guide; they do not require a separate approval step.

## Project Structure

### File Per Type

Each primary production type gets its own file, with its constructors and all its
production methods. Small supporting types may stay with the primary type when
they exist only to support it.

Tests belong in corresponding `_test.go` files. Generated code and platform/build
constraints may require separate files; use those mechanisms when needed, rather
than splitting methods just to shorten a file. Match nearby file naming instead
of renaming unrelated files.

### Package Responsibilities

Keep database behavior in the root `mojura` package, filter descriptions in
`filters`, and binary history actions in `action`. Executable examples belong under
`examples`; see [examples/basic](examples/basic). Preserve dependency direction:
`mojura` imports `filters` and `action`, so those packages must not import `mojura`.

Keep application value types and domain rules in the consuming application or its
library. Define their relationship mappings beside the value type. Mojura provides
storage and indexes; it does not own application authorization, HTTP handlers, or
foreign-key validation. No package-level registration or plugin lifecycle exists.

### Constructor Placement

Place constructors directly above the type they construct, followed by its methods.
Methods such as `New` on an existing database, `Reindex`, and `Close` are not
constructors of the receiver type.

This keeps entry points predictable and related behavior together.

For constructors returning pointers, prefer a local value declared with `var`,
explicit field assignments, and returning its address. Name the pointer result `out`.

For example, in `counter.go`:

```go
// counter.go

// NewCounter constructs a Counter starting at initial.
func NewCounter(initial int) (out *Counter) {
	var c Counter
	c.value = initial
	return &c
}

// Counter tracks a count. It is not safe for concurrent use.
type Counter struct {
	value int
}

// Increment increases the count by one.
func (c *Counter) Increment() {
	c.value++
}
```

Do not create `service_helpers.go` or `service_utils.go` to scatter methods of the
same type. When responsibilities grow, extract a cohesive type instead.

## Formatting

* Run `gofmt` on changed Go files and use its formatting in documentation examples.
* Group standard-library imports separately from third-party imports.
* Use blank lines to separate logical steps, not every statement.
* Always insert one blank line after a complete `if`, `for`, or `switch` statement
  before the next statement in the same block. This includes `for range` loops,
  type switches, and control-flow blocks that end with an early return.
* Treat an `if` / `else if` / `else` chain as one statement: keep `} else {` together
  and put the blank line after the complete chain. No trailing blank line is needed
  when only the enclosing block's closing brace follows.

Add this spacing explicitly; `gofmt` does not insert the required blank lines.

## Naming

* Use descriptive, intention-revealing names.
* Avoid unnecessary abbreviations.
* Use verbs for functions (Build, Parse, Fetch).
* Use nouns for types (Parser, Client, Store).
* Use familiar short names such as `i`, `err`, `ctx`, and `ok` in narrow scopes.
* Preserve Go initialisms, such as `EntryID`, `JSONEncoder`, and `ID`.
* Do not rename public symbols solely to improve style or remove stutter.

Avoid stutter:

* Preferred: `type Client struct{}`
* Avoid: `type MyProjectClient struct{}`

## Variable Declarations

### Prefer var over := (with narrow exceptions)

Prefer `var` for zero values, explicit types, and related values reused across
branches. For example, this declaration fragment:

```go
var (
	count int
	ids   []string
	err   error
)
```

Group related declarations near their first use; do not hoist every local to the
beginning of a function.

Use `:=` only when it meaningfully improves clarity in tight scopes and the
declaration is simple. Loop variables and scoped lookups fit this exception too.

`:=` is acceptable when all of the following are true:

* The declaration is local and close to first use.
* The right-hand side is short and obvious.
* The variable has no ambiguity in meaning or type.
* The statement does not risk shadowing an existing variable.

Simple declaration fragments:

```go
done := make(chan struct{}, 1)
timeout := 5 * time.Second
```

When a value needs to survive a branch, declare it outside and assign with `=`.
For example, inside a function returning `([]byte, error)`:

```go
var (
	data []byte
	err  error
)

if data, err = os.ReadFile(filename); err != nil {
	return nil, fmt.Errorf("read configuration %q: %w", filename, err)
}

return data, nil
```

Scoped lookup fragment:

```go
if v, ok := m[key]; ok {
	return v, nil
}
```

### Avoid Shadowing

Do not shadow an existing local, parameter, receiver, or named result. An inner
`:=` can introduce a new variable even if the name already exists in an outer scope.

Avoid:

```go
// Incorrect: the inner err hides the result, so the caller receives nil.
func update() (err error) {
	if err := save(); err != nil {
		log.Print(err)
	}

	return err
}
```

Assign to the existing result and handle the error immediately:

```go
func update() (err error) {
	if err = save(); err != nil {
		return fmt.Errorf("save update: %w", err)
	}

	return nil
}
```

## Named Returns

### Named Returns Are Encouraged

Prefer named results when their names explain non-obvious outputs or distinguish
multiple values. They are encouraged for public APIs, but are not mandatory when
names add no information. Name all results in a signature or leave all unnamed.

### No Naked Returns

Never use a naked return in a function that returns values, including in early
exits. Explicit values make control flow easier to review and refactor. Bare
`return` is acceptable in any function or closure with no results.

Avoid:

```go
// Incorrect: the returned values are implicit.
func parseCount(input string) (count int, err error) {
	count, err = strconv.Atoi(input)
	return
}
```

Prefer:

```go
func parseCount(input string) (count int, err error) {
	if count, err = strconv.Atoi(input); err != nil {
		return 0, fmt.Errorf("parse count %q: %w", input, err)
	}

	return count, nil
}
```

Named results can be changed by deferred functions even after an explicit return.
Use that behavior deliberately for cleanup, keep it visible, and never shadow the
result being updated.

## Function Design

Apply the [unit philosophy](#unit-philosophy-small-and-simple) to every new or
substantially changed function. Keep functions short enough to understand at a
glance, with a name that describes their entire job. Extract a step when it has its
own purpose, inputs, or error handling. There is no fixed line-count target.

Keep coordinating functions focused on call order and error propagation. Put each
operation's details and error context in the function that performs it. Keep
ownership and cleanup visible when splitting a function.

The [basic example](examples/basic/main.go) demonstrates this structure: `run` owns
the temporary directory, `runDatabase` owns the database lifetime, and `runCRUD`
orders small functions for creating, reading, filtering, changing, iterating, and
deleting a record. Each operation can be understood independently.

Use early returns to reduce nesting. For example, this statement fragment:

```go
if err != nil {
	return resp, err
}
```

## Value, Backend, and History Responsibilities

Keep value types responsible for metadata accessors and stable relationship slots.
Prefer embedding `Entry` by value, and preserve the `Value` contract, including its
legacy method, when changing types. Return fresh relationship slices when mutation
could otherwise change the index snapshot captured before `Update`.

Keep transaction storage changes, index changes, next-ID metadata, and history
recording coordinated. Backend and Kiroku commits are separate; do not introduce
stronger atomicity or shutdown claims than the implementation supports. A custom
backend must meet the nested-bucket and ordered-cursor contract in the pinned
`github.com/mojura/backend` dependency.

Keep request handling and application domain rules outside Mojura. Use ordinary Go
values and returned errors at those boundaries. See [usage](docs/usage.md) and
[configuration](docs/configuration.md) for the implemented responsibilities.

## Error Handling

* Handle errors before using the associated result.
* For external operation errors (I/O, filesystem, network, system calls), add useful
  operation context and preserve the cause with `%w` when returning the error.
* Include a relevant identifier or path when appropriate. Avoid secrets and whole
  payloads, and do not repeat identical context at every layer.
* Return descriptive sentinel contract/state errors directly where the API expects
  them. Use `errors.Is` or `errors.As` for wrapped errors in new code, rather than
  comparing message text to identify a cause or type.
* Preserve existing error contracts. Changing `%v` to `%w` exposes a cause to callers
  and can change behavior; do not treat that as incidental formatting cleanup.

Contextual error fragment:

```go
if err != nil {
	return out, fmt.Errorf("load user %q: %w", id, err)
}
```

Return errors from library code. Prefer logging once at the boundary that handles
the failure; do not both log and return the same error without a specific reason.
Reserve `log.Fatal` and `os.Exit` for executable entry points, since they bypass
normal caller cleanup. Do not use panics for ordinary operational failures.

Sentinel check fragment:

```go
if entryID == "" {
	return ErrEmptyEntryID
}
```

Use the actual sentinel promised by the API. Some Mojura error constants are legacy
declarations that are not returned by current code.

### Resource Ownership

The code that acquires a resource owns cleanup unless it explicitly transfers
ownership. Arrange cleanup after successful acquisition, including when later
initialization fails. Check meaningful `Close`, flush, and final write errors.
If both the operation and cleanup fail, preserve the original failure and handle
the cleanup failure too. Explain an intentional ignored cleanup error briefly.

For example, this function preserves write and close errors using the
standard-library `errors` package:

```go
func writeState(filename string, data []byte) (err error) {
	var file *os.File
	if file, err = os.Create(filename); err != nil {
		return fmt.Errorf("create state %q: %w", filename, err)
	}

	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close state %q: %w", filename, closeErr))
		}
	}()

	if _, err = file.Write(data); err != nil {
		return fmt.Errorf("write state %q: %w", filename, err)
	}

	return nil
}
```

This demonstrates error handling, not atomic file replacement. If a file already
imports `github.com/gdbu/errors`, distinguish it from the standard library with a
clear alias when both are needed.

## Interfaces

* Prefer accepting small interfaces when they express a real dependency boundary.
  Do not create an interface for every concrete type.
* Prefer returning concrete types from constructors. Preserve required contracts
  such as `backend.Initializer.New(string) (backend.Backend, error)`,
  `Encoder`, and the generic `Value` constraint.
* Keep interfaces small and behavior-focused.
* Define interfaces near where they are used.

## Receivers

* Use pointer receivers when mutating structs, when copying would be expensive,
  or when a struct contains synchronization primitives.
* Keep receiver choices consistent for a type and compatible with its interfaces.
  Do not copy a value containing a mutex after it has been used.
* Keep receiver names short (s, c, p).
* Never use `this` or `self` as receiver names.

## Comments

* All exported types, functions, methods, constants, and variables must have Go doc
  comments. Document public fields and interface methods when their contract needs
  explanation.
* Go doc comments must begin with the name of the exported symbol.
* API comments describe behavior and contracts: defaults, errors, side effects,
  ownership, and concurrency guarantees where relevant. Implementation comments
  explain non-obvious decisions and constraints.
* Keep comments concise and avoid restating obvious code, but do not omit necessary
  behavior to meet a line limit.
* Comment private helpers when the comment adds information or aids navigation.
* Keep documentation examples consistent with the guide. Label incorrect examples
  and code fragments explicitly. Do not promise that an unimplemented feature works.

The `Counter` example above documents behavior and keeps the constructor above the
type. Update the relevant guide when that behavior changes.

Avoid this redundant comment fragment:

```go
// i increments by 1.
i++
```

## Tests

* Use the standard `testing` package. Prefer table-driven tests for related cases;
  a single focused test does not need a table.
* Keep setup explicit.
* Avoid clever test abstractions.
* Use descriptive case names and `t.Run` where separate case output helps.
* Use `t.Helper`, `t.TempDir`, and `t.Cleanup` where appropriate for diagnostics,
  temporary files, and test-owned resources.
* Assert observable behavior and relevant error cases. Add focused regression
  coverage for bug fixes, not tests that merely mirror trivial declarations.
* Do not use `t.Parallel` for existing tests that share `./test_data`, or run separate
  test processes concurrently in the same checkout. Use separate temporary data
  directories and database instances for new tests. See
  [docs/development.md](docs/development.md) for isolation guidance.

For related cases, follow the table-driven tests in
[encryptedJSONEncoder_test.go](encryptedJSONEncoder_test.go) and the cursor test
files, while applying this guide to newly written code. Existing tests do not all
meet these conventions; do not copy shared-directory setup into new tests when
`t.TempDir` can isolate the data.

## Pull Request Descriptions

Use [PULL_REQUEST_TEMPLATE.md](PULL_REQUEST_TEMPLATE.md) for every
pull request description. Preserve its Summary, Changes, and Testing sections and
replace the placeholders with details relevant to the change. In Testing, report
the checks actually run and any limitations.

## PR Checklist

Apply this checklist to the scope of the change:

* [ ] Pull request description follows [PULL_REQUEST_TEMPLATE.md](PULL_REQUEST_TEMPLATE.md)
* [ ] `gofmt` applied
* [ ] Completed `if`, `for`, and `switch` statements are separated from following statements by a blank line
* [ ] One file per primary type
* [ ] Production methods stay with their type, except for documented build/generated cases
* [ ] Constructors appear directly above their type
* [ ] Each function, type, and package has one clear responsibility
* [ ] Functions are small and focused; coordinators use named operations
* [ ] Helpers reduce complexity without unnecessary indirection or shared state
* [ ] Named returns used appropriately
* [ ] No naked returns
* [ ] Declarations use `var` or a documented `:=` exception
* [ ] No shadowing
* [ ] External/operation errors include context
* [ ] Sentinel/state errors follow the API's contract
* [ ] Existing error contracts and resource ownership are preserved
* [ ] Exported symbols have Go doc comments
* [ ] Tests cover meaningful behavior changes and isolate shared state
* [ ] Relevant checks from [docs/development.md](docs/development.md) passed, or limitations are recorded
* [ ] Unrelated style changes are excluded and pre-existing user edits are preserved
