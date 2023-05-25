package logger

import (
	"encoding/json"
	"os"

	"github.com/piyushsingariya/syndicate/constants"
	"github.com/piyushsingariya/syndicate/models"
)

func LogSpec(spec map[string]interface{}) {
	message := models.Message{}
	message.Spec = spec
	message.Type = constants.SpecType

	json.NewEncoder(os.Stdout).Encode(message)
}
