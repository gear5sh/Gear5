package logger

import (
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"time"

	"github.com/goccy/go-json"

	"github.com/piyushsingariya/kaku/models"
	"github.com/piyushsingariya/kaku/types"
)

func LogRecord(record models.Record) {
	message := models.Message{}
	message.Type = types.RecordType
	message.Record = &record
	message.Record.EmittedAt = time.Now()

	err := json.NewEncoder(writer).Encode(message)
	if err != nil {
		Fatalf("failed to encode record %v: %s", record, err)
	}
}

func LogSpec(spec map[string]interface{}) {
	message := models.Message{}
	message.Spec = spec
	message.Type = types.SpecType

	Info("logging spec")
	err := json.NewEncoder(writer).Encode(message)
	if err != nil {
		Fatalf("failed to encode spec %v: %s", spec, err)
	}
}

func LogCatalog(streams []*models.Stream) {
	message := models.Message{}
	message.Type = types.CatalogType
	message.Catalog = models.GetWrappedCatalog(streams)
	Info("logging catalog")
	err := json.NewEncoder(writer).Encode(message)
	if err != nil {
		Fatalf("failed to encode catalog %v: %s", streams, err)
	}
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

	err = json.NewEncoder(writer).Encode(message)
	if err != nil {
		Fatalf("failed to encode connection status: %s", err)
	}
}

func LogResponse(response *http.Response) {
	respDump, err := httputil.DumpResponse(response, true)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(string(respDump))
}

func LogRequest(req *http.Request) {
	requestDump, err := httputil.DumpRequest(req, true)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(string(requestDump))
}

func LogState(state *models.State) {
	message := models.Message{}
	message.Type = types.StateType
	message.State = state

	err := json.NewEncoder(writer).Encode(message)
	if err != nil {
		Fatalf("failed to encode connection status: %s", err)
	}
}
