package mojura_test

import (
	"fmt"
	"os"

	"github.com/mojura/mojura"
)

func ExampleNew() {
	var (
		dir string
		err error
	)
	if dir, err = os.MkdirTemp("", "mojura-example-"); err != nil {
		fmt.Println("create directory:", err)
		return
	}

	defer func() {
		if removeErr := os.RemoveAll(dir); removeErr != nil {
			fmt.Println("remove directory:", removeErr)
		}
	}()

	// Entry itself satisfies Value when no relationship indexes are needed.
	var db *mojura.Mojura[*mojura.Entry]
	opts := mojura.MakeOpts("entries", dir)
	if db, err = mojura.New[*mojura.Entry](opts); err != nil {
		fmt.Println("open database:", err)
		return
	}

	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			fmt.Println("close database:", closeErr)
		}
	}()

	var created *mojura.Entry
	if created, err = db.New(&mojura.Entry{}); err != nil {
		fmt.Println("create entry:", err)
		return
	}

	fmt.Println("Created", created.ID)
	// Output: Created 00000000
}
