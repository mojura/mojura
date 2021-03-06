package mojura

// Note: This is to support the deprecated GetRelationshipIDs method.
// This will be removed in v0.6.0
func newRelationshipsFromIDs(relationshipIDs []string) (r Relationships) {
	for _, id := range relationshipIDs {
		r.Append(id)
	}

	return
}

// Relationships help to store the relationships for an Entry
type Relationships []Relationship

// Append will append a set of relationship IDs for a given relationship
func (r *Relationships) Append(relationshipIDs ...string) {
	*r = append(*r, relationshipIDs)
}

// Relationship help to store the IDs for an Entry's relationship
type Relationship []string

// Has will note if a relationship has a particular relationship ID
func (r Relationship) Has(relationshipID string) (has bool) {
	for _, id := range r {
		if id == relationshipID {
			return true
		}
	}

	return false
}

func (r Relationship) delta(old Relationship, onAdd, onRemove RelationshipFn) (err error) {
	if err = r.addNew(old, onAdd); err != nil {
		return
	}

	if err = r.removeOld(old, onRemove); err != nil {
		return
	}

	return
}

func (r Relationship) addNew(old Relationship, onAdd RelationshipFn) (err error) {
	for _, id := range r {
		if old.Has(id) {
			continue
		}

		if err = onAdd([]byte(id)); err != nil {
			return
		}
	}

	return
}

func (r Relationship) removeOld(old Relationship, onRemove RelationshipFn) (err error) {
	for _, id := range old {
		if r.Has(id) {
			continue
		}

		if err = onRemove([]byte(id)); err != nil {
			return
		}
	}

	return
}

// RelationshipFn is called for relationship comparison funcs
type RelationshipFn func(relationshipID []byte) error
