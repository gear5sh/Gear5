package driver

import (
	"fmt"

	"github.com/jmoiron/sqlx"
	kakumodels "github.com/piyushsingariya/kaku/models"
	"github.com/piyushsingariya/kaku/types"
	"github.com/piyushsingariya/kaku/utils"
)

type pgStream struct {
	batchSize int64
	cursor    string
	state     interface{}
	*kakumodels.Stream
}

const (
	readRecordsFullRefresh = "SELECT * FROM $1.$2 OFFSET $3 LIMIT $4"
)

func (p *pgStream) setState(cursor string, stateMap map[string]any) {

}

func (p *pgStream) readFullRefresh(client *sqlx.DB, channel chan<- kakumodels.Record) error {
	offset := int64(0)
	limit := p.batchSize

	for {
		var recordOutput []types.RecordData
		err := client.Select(&recordOutput, readRecordsFullRefresh, p.Namespace, p.Name, offset*limit, limit)
		if err != nil {
			return fmt.Errorf("failed to read after offset[%d] limit[%d]: %s", offset*limit, limit, err)
		}

		// records finished
		if len(recordOutput) == 0 {
			break
		}

		for _, record := range recordOutput {
			// insert record
			channel <- utils.ReformatRecord(p.Name, p.Namespace, record)
		}
	}
	return nil
}

func (p *pgStream) readIncremental(client *sqlx.DB, channel chan<- kakumodels.Record) error {
	offset := int64(0)
	limit := p.batchSize
	var localState any

	if p.state != nil {
		// read incrementally

		return nil
	}

	for {
		var recordOutput []types.RecordData
		err := client.Select(&recordOutput, readRecordsFullRefresh, p.Namespace, p.Name, offset*limit, limit)
		if err != nil {
			return fmt.Errorf("failed to read after offset[%d] limit[%d]: %s", offset*limit, limit, err)
		}

		// records finished
		if len(recordOutput) == 0 {
			break
		}

		for _, record := range recordOutput {
			if cursorVal, found := record[p.cursor]; found && cursorVal != nil {
				// compare if not nil
				if localState != nil {
					state, err := utils.MaximumOnDataType(p.JSONSchema.Properties[p.cursor].Type, localState, cursorVal)
					if err != nil {
						return err
					}

					localState = state
				} else {
					// directly update
					localState = cursorVal
				}
			}

			// insert record
			channel <- utils.ReformatRecord(p.Name, p.Namespace, record)
		}
	}

	// update state
	p.state = localState

	return nil
}
