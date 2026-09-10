// Package filters constructs the filter descriptions accepted by Mojura.
// Filters select indexed relationship IDs; comparisons with an empty relationship
// key select entry IDs. Comparisons use lexical string ordering. Mojura intersects
// filters and uses the first filter to drive traversal, which affects ordering and
// inverse-filter semantics. See the repository usage guide for cursor limitations.
package filters
