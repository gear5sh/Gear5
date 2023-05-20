package main

import (
	"fmt"
	"log"

	"github.com/piyushsingariya/syndicate/drivers/google-sheets/models"
	"github.com/piyushsingariya/syndicate/utils"
)

func main() {
	model := &models.Config{}
	schema, err := utils.ToJsonSchema(model)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(schema)
	// model.GetSchema()
}
