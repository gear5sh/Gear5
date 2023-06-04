package logger

import (
	"encoding/json"

	"github.com/piyushsingariya/syndicate/models"
	"github.com/piyushsingariya/syndicate/types"
)

func LogRecord(record models.RecordRow) {
	message := models.Message{}
	message.Type = types.RecordType
	message.Record = &record

	json.NewEncoder(writer).Encode(message)
}

func LogSpec(spec map[string]interface{}) {
	message := models.Message{}
	message.Spec = spec
	message.Type = types.SpecType

	Info("logging spec")
	json.NewEncoder(writer).Encode(message)
}

func LogCatalog(streams []*models.Stream) {
	message := models.Message{}
	message.Type = types.CatalogType
	message.Catalog = &models.Catalog{
		Streams: []*models.WrappedStream{},
	}

	for _, stream := range streams {
		message.Catalog.Streams = append(message.Catalog.Streams, &models.WrappedStream{
			Stream: stream,
		})
	}

	Info("logging catalog")
	json.NewEncoder(writer).Encode(message)
}

func LogConnectionStatus(err error) {
	message := models.Message{}
	message.Type = types.ConnectionStatusType
	message.ConnectionStatus = &models.StatusRow{}
	if err != nil {
		message.ConnectionStatus.Message = err.Error()
		message.ConnectionStatus.Status = types.ConnectionFailed
	} else {
		message.ConnectionStatus.Status = types.ConnectionSucceed
	}

	json.NewEncoder(writer).Encode(message)
}
