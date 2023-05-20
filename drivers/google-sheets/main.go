package main

import (
	"fmt"
	"log"

	"github.com/piyushsingariya/syndicate/drivers/google-sheets/models"
	"github.com/piyushsingariya/syndicate/utils"
)

func main() {
	schema, err := utils.ToJsonSchema(models.Config{})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(schema)
}
