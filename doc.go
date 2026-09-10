// Package mojura stores generic Go values with relationship indexes in a
// transactional backend. The default backend is Bolt; Kiroku handles history,
// snapshots, and optional replication sources.
//
// Embed Entry by value in a concrete type, implement GetRelationships with fixed
// positional slots, and open the database with New using that type's pointer.
// Create the data directory first and close each successfully opened database.
// See the repository's docs/usage.md, docs/configuration.md, and docs/development.md
// for query semantics, effective defaults, and known implementation limitations.
package mojura
