// Command basic demonstrates Mojura using an automatically removed temporary database.
package main

import (
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/mojura/mojura"
	"github.com/mojura/mojura/filters"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() (err error) {
	var dir string
	if dir, err = os.MkdirTemp("", "mojura-basic-"); err != nil {
		return fmt.Errorf("create example directory: %w", err)
	}

	defer func() {
		if removeErr := os.RemoveAll(dir); removeErr != nil {
			err = errors.Join(err, fmt.Errorf("remove example directory: %w", removeErr))
		}
	}()

	return runDatabase(dir)
}

func runDatabase(dir string) (err error) {
	var db *mojura.Mojura[*record]
	opts := mojura.MakeOpts("messages", dir)
	if db, err = mojura.New[*record](opts, "owners", "tags"); err != nil {
		return fmt.Errorf("open messages: %w", err)
	}

	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close messages: %w", closeErr))
		}
	}()

	return runCRUD(db)
}

func runCRUD(db *mojura.Mojura[*record]) (err error) {
	var created *record
	if created, err = createRecord(db); err != nil {
		return err
	}

	if err = showRecord(db, created.ID); err != nil {
		return err
	}

	if err = showOwnerMatches(db, created.OwnerID); err != nil {
		return err
	}

	var updated *record
	if updated, err = updateRecord(db, created.ID); err != nil {
		return err
	}

	if err = replaceRecord(db, updated); err != nil {
		return err
	}

	if err = showRecords(db); err != nil {
		return err
	}

	return deleteRecord(db, created.ID)
}

func createRecord(db *mojura.Mojura[*record]) (created *record, err error) {
	var value record
	value.OwnerID = "user_1"
	value.Tags = []string{"go", "databases"}
	value.Message = "Hello, Mojura!"

	if created, err = db.New(&value); err != nil {
		return nil, fmt.Errorf("create message: %w", err)
	}

	fmt.Printf("Created %s\n", created.ID)
	return created, nil
}

func showRecord(db *mojura.Mojura[*record], id string) (err error) {
	var fetched *record
	if fetched, err = db.Get(id); err != nil {
		return fmt.Errorf("get message: %w", err)
	}

	fmt.Printf("Read: %s\n", fetched.Message)
	return nil
}

func showOwnerMatches(db *mojura.Mojura[*record], ownerID string) (err error) {
	query := mojura.NewFilteringOpts(filters.Match("owners", ownerID))
	var matches []*record
	if matches, _, err = db.GetFiltered(query); err != nil {
		return fmt.Errorf("filter messages: %w", err)
	}

	fmt.Printf("Owner matches: %d\n", len(matches))
	return nil
}

func updateRecord(db *mojura.Mojura[*record], id string) (updated *record, err error) {
	if updated, err = db.Update(id, func(value *record) error {
		value.Message = "Updated message"
		return nil
	}); err != nil {
		return nil, fmt.Errorf("update message: %w", err)
	}

	fmt.Printf("Updated: %s\n", updated.Message)
	return updated, nil
}

func replaceRecord(db *mojura.Mojura[*record], value *record) (err error) {
	value.Message = "Replaced message"
	if value, err = db.Put(value.ID, value); err != nil {
		return fmt.Errorf("replace message: %w", err)
	}

	fmt.Printf("Put: %s\n", value.Message)
	return nil
}

func showRecords(db *mojura.Mojura[*record]) (err error) {
	if err = db.ForEach(func(id string, value *record) error {
		fmt.Printf("Iterated %s: %s\n", id, value.Message)
		return nil
	}, nil); err != nil {
		return fmt.Errorf("iterate messages: %w", err)
	}

	return nil
}

func deleteRecord(db *mojura.Mojura[*record], id string) (err error) {
	if _, err = db.Delete(id); err != nil {
		return fmt.Errorf("delete message: %w", err)
	}

	var exists bool
	if exists, err = db.Exists(id); err != nil {
		return fmt.Errorf("check deleted message: %w", err)
	}

	fmt.Printf("Exists after delete: %t\n", exists)
	return nil
}
