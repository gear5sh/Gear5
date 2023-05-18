package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/piyushsingariya/syndicate/drivers/google-sheets/models"
	"github.com/swaggest/jsonschema-go"
)

func main() {
	reflector := jsonschema.Reflector{}

	schema, err := reflector.Reflect(models.Config{})
	if err != nil {
		log.Fatal(err)
	}

	j, err := json.MarshalIndent(schema, "", " ")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(string(j))
}
