package mojura

import (
	"context"
	"testing"
)

// BenchmarkForEachIntersectedID measures an intersection beside the
// multi-filter cursor walking the same filters, its largest set first, in
// the same process: a selective pair (20,000 and 10), the same pair stopped
// at its first ID, a dense pair (20,000 and 20,000) and an interleaved,
// disjoint pair (10,000 and 10,000) (INT-MOJURA-TICKET-5, AC-6).
func BenchmarkForEachIntersectedID(b *testing.B) {
	for _, shape := range []struct {
		name  string
		tags  func(i int) []string
		limit bool
	}{
		{name: "selective", tags: func(i int) []string {
			if i%2000 == 7 {
				return []string{"large", "small"}
			}
			return []string{"large"}
		}},
		{name: "limit-1", limit: true, tags: func(i int) []string {
			if i%2000 == 7 {
				return []string{"large", "small"}
			}
			return []string{"large"}
		}},
		{name: "dense", tags: func(int) []string { return []string{"large", "small"} }},
		{name: "disjoint", tags: func(i int) []string {
			if i%2 == 0 {
				return []string{"large"}
			}
			return []string{"small"}
		}},
	} {
		b.Run(shape.name, func(b *testing.B) {
			c := openTempMojura(b)
			seedTagged(b, c, 20000, shape.tags)
			fs := tag("large", "small")
			visit := func(string) error {
				if shape.limit {
					return Break
				}
				return nil
			}
			b.Run("intersection", func(b *testing.B) {
				b.ReportAllocs()
				if err := c.ReadTransaction(context.Background(), func(txn *Transaction[*testStruct]) error {
					for b.Loop() {
						if err := txn.ForEachIntersectedID(visit, fs...); err != nil {
							return err
						}
					}
					return nil
				}); err != nil {
					b.Fatal(err)
				}
			})
			b.Run("multi-filter cursor", func(b *testing.B) {
				b.ReportAllocs()
				if err := c.ReadTransaction(context.Background(), func(txn *Transaction[*testStruct]) error {
					for b.Loop() {
						cursor, err := txn.IDCursor(fs...)
						if err != nil {
							return err
						}
						for id, err := cursor.First(); ; id, err = cursor.Next() {
							if err == Break {
								break
							}
							if err != nil {
								return err
							}
							if visit(id) == Break {
								break
							}
						}
					}
					return nil
				}); err != nil {
					b.Fatal(err)
				}
			})
		})
	}
}
