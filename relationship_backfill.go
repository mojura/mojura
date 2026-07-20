package mojura

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/mojura/backend"
)

// RelationshipBackfillPage reports one committed additive index batch.
type RelationshipBackfillPage struct {
	Relationship string
	AfterID      string
	LastID       string
	Scanned      int
	Indexed      int
	Done         bool
}

// BackfillRelationship adds one relationship index in a bounded transaction.
// Existing entries and unrelated relationship indexes are left untouched.
func (m *Mojura[T]) BackfillRelationship(ctx context.Context, relationship, afterID string, limit int) (page RelationshipBackfillPage, err error) {
	relationship = strings.TrimSpace(relationship)
	afterID = strings.TrimSpace(afterID)
	page = RelationshipBackfillPage{
		Relationship: relationship,
		AfterID:      afterID,
		LastID:       afterID,
	}
	if err = ctx.Err(); err != nil {
		return
	}
	if relationship == "" {
		err = fmt.Errorf("relationship is required")
		return
	}
	if limit <= 0 {
		err = fmt.Errorf("relationship backfill limit must be greater than zero")
		return
	}
	relationshipIndex := -1
	for i, key := range m.relationships {
		if string(key) == relationship {
			relationshipIndex = i
			break
		}
	}
	if relationshipIndex < 0 {
		err = fmt.Errorf("%w: %s", ErrRelationshipNotFound, relationship)
		return
	}

	err = m.db.Transaction(func(backendTxn backend.Transaction) error {
		txn := newTransaction(ctx, m, backendTxn, nil)
		entries, getErr := txn.getEntriesBucket()
		if getErr != nil {
			return getErr
		}
		cursor := entries.Cursor()
		entryID, raw := cursor.First()
		if afterID != "" {
			entryID, raw = cursor.Seek([]byte(afterID))
			if entryID != nil && bytes.Compare(entryID, []byte(afterID)) <= 0 {
				entryID, raw = cursor.Next()
			}
		}
		for entryID != nil && page.Scanned < limit {
			if err := ctx.Err(); err != nil {
				return err
			}
			value, decodeErr := m.newValueFromBytes(raw)
			if decodeErr != nil {
				return fmt.Errorf("decode entry %q for relationship backfill: %w", entryID, decodeErr)
			}
			relationships := value.GetRelationships()
			if len(relationships) != len(m.relationships) {
				return ErrInvalidNumberOfRelationships
			}
			for _, relationshipID := range relationships[relationshipIndex] {
				if setErr := txn.setRelationship(m.relationships[relationshipIndex], []byte(relationshipID), entryID); setErr != nil {
					return setErr
				}
				page.Indexed++
			}
			page.Scanned++
			page.LastID = string(entryID)
			entryID, raw = cursor.Next()
		}
		page.Done = entryID == nil
		return nil
	})
	if err != nil {
		page.LastID = afterID
		page.Scanned = 0
		page.Indexed = 0
		page.Done = false
	}
	return
}
