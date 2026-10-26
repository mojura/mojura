package mojura

import "bytes"

// walkIntersection calls fn with each ID every cursor holds, in ascending
// order: a cursor behind the highest current ID steps once, then seeks if
// that fell short. fn returning Break, or a cursor running out, ends the walk
// with nil.
func walkIntersection(cursors []filterCursor, fn ForEachIDFn) error {
	ids := make([][]byte, len(cursors))
	for i, cursor := range cursors {
		id, err := cursor.First()
		if err != nil {
			return endOfIntersection(err)
		}

		ids[i] = id
	}

	// target is the highest cursor's key, held in place: the cursor holding
	// it does not move while the others step to it, and no fn runs between.
	// Only a yield copies, into next, the one buffer the walk grows.
	target := maxKey(ids)
	var next []byte
	for {
		aligned := true
		for i, cursor := range cursors {
			if bytes.Equal(ids[i], target) {
				continue
			}

			aligned = false
			id, err := stepTo(cursor, target)
			if err != nil {
				return endOfIntersection(err)
			}

			ids[i] = id
		}

		if !aligned {
			target = maxKey(ids)
			continue
		}

		// The key just past target, copied before fn can touch the
		// transaction: every cursor steps beyond the ID it yielded in the
		// same loop that aligns them.
		next = append(append(next[:0], target...), 0)
		if err := fn(string(target)); err != nil {
			return endOfIntersection(err)
		}

		target = next
	}
}

// stepTo moves cursor to the first ID at or after target: one step, which
// reaches it where the sets interleave, then a seek if that fell short.
func stepTo(cursor filterCursor, target []byte) ([]byte, error) {
	id, err := cursor.Next()
	if err != nil || bytes.Compare(id, target) >= 0 {
		return id, err
	}

	return cursor.SeekForward(nil, target)
}

func maxKey(ids [][]byte) []byte {
	top := ids[0]
	for _, id := range ids[1:] {
		if bytes.Compare(id, top) > 0 {
			top = id
		}
	}

	return top
}

// endOfIntersection is a walk's result: Break, from a cursor that ran out
// or from fn, ends it with nil.
func endOfIntersection(err error) error {
	if err == Break {
		return nil
	}

	return err
}
