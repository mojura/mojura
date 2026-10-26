package mojura

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/mojura/backend"
)

var (
	// ErrInvalidSortKey reports an invalid sort-key width or use of sorted reads on an unsorted value.
	ErrInvalidSortKey = errors.New("invalid relationship sort key")
	// ErrRelationshipRebuildRequired reports a persisted layout that needs explicit conversion.
	ErrRelationshipRebuildRequired = errors.New("relationship layout requires explicit rebuild")
	// ErrRelationshipRebuildInProgress rejects indexed reads and writes during conversion.
	ErrRelationshipRebuildInProgress = errors.New("relationship rebuild is in progress")
	// ErrRelationshipRebuildAborting requires finishing abort before restarting conversion.
	ErrRelationshipRebuildAborting = errors.New("relationship rebuild abort must finish")
	relationshipLayoutKey          = []byte("relationship_layout")
	relationshipAlternateKey       = []byte("relationships_next")
)

type relationshipLayout struct {
	Width    int                `json:"width"`
	Slot     int                `json:"slot"`
	Keys     []string           `json:"keys,omitempty"`
	Build    *relationshipBuild `json:"build,omitempty"`
	Retiring bool               `json:"retiring,omitempty"`
}

type relationshipBuild struct {
	Width    int      `json:"width"`
	Keys     []string `json:"keys"`
	After    string   `json:"after,omitempty"`
	Aborting bool     `json:"aborting,omitempty"`
}

func (l *relationshipLayout) root() []byte {
	if l.Slot == 1 {
		return relationshipAlternateKey
	}

	return relationshipsBktKey
}

func (l *relationshipLayout) otherRoot() []byte {
	if l.Slot == 1 {
		return relationshipsBktKey
	}

	return relationshipAlternateKey
}

func loadRelationshipLayout(tx backend.Transaction) (*relationshipLayout, error) {
	meta := tx.GetBucket(metaBktKey)
	if meta == nil {
		return nil, ErrNotInitialized
	}

	raw := meta.Get(relationshipLayoutKey)
	if raw == nil {
		return nil, nil
	}

	var layout relationshipLayout
	if err := json.Unmarshal(raw, &layout); err != nil {
		return nil, fmt.Errorf("read relationship layout: %w", err)
	}

	if layout.Width < 0 || (layout.Slot != 0 && layout.Slot != 1) || (layout.Build != nil && (layout.Build.Width < 0 || layout.Retiring)) {
		return nil, errors.New("invalid relationship layout")
	}

	return &layout, nil
}

func saveRelationshipLayout(tx backend.Transaction, layout *relationshipLayout) error {
	raw, err := json.Marshal(layout)
	if err != nil {
		return err
	}

	return tx.GetBucket(metaBktKey).Put(relationshipLayoutKey, raw)
}

// RelationshipRebuildPage describes only the batch which committed.
// Scanned counts entries built; Removed counts retired keys or empty buckets.
type RelationshipRebuildPage struct {
	Scanned int
	Removed int
	AfterID string
	Phase   string
	Done    bool
}

func retireRelationships(ctx context.Context, tx backend.Transaction, rootKey []byte, limit int) (int, bool, error) {
	for removed := 0; removed < limit; removed++ {
		if err := ctx.Err(); err != nil {
			return 0, false, err
		}

		root := tx.GetBucket(rootKey)
		if root == nil {
			return removed, true, nil
		}

		name, _ := root.Cursor().First()
		if name == nil {
			return removed + 1, true, tx.DeleteBucket(rootKey)
		}

		parent := root.GetBucket(name)
		if parent == nil {
			return 0, false, errors.New("invalid retiring relationship bucket")
		}

		value, _ := parent.Cursor().First()
		if value == nil {
			if err := root.DeleteBucket(name); err != nil {
				return 0, false, err
			}

			continue
		}

		bucket := parent.GetBucket(value)
		if bucket == nil {
			return 0, false, errors.New("invalid retiring relationship membership")
		}

		key, _ := bucket.Cursor().First()
		if key == nil {
			if err := parent.DeleteBucket(value); err != nil {
				return 0, false, err
			}
		} else if err := bucket.Delete(key); err != nil {
			return 0, false, err
		}
	}

	return limit, false, nil
}
