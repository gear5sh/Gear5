package waljs

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/cloudquery/plugin-sdk/v4/scalar"
	"github.com/piyushsingariya/shift/protocol"
	"github.com/piyushsingariya/shift/utils"
)

type ChangeFilter struct {
	tables map[string]*arrow.Schema
}

type Filtered func(change Wal2JsonChanges)

func NewChangeFilter(streams ...protocol.Stream) ChangeFilter {
	filter := ChangeFilter{
		tables: make(map[string]*arrow.Schema),
	}

	for _, stream := range streams {
		filter.tables[stream.ID()] = stream.Schema().ToArrow()
	}

	return filter
}

func (c ChangeFilter) FilterChange(lsn string, change []byte, OnFiltered Filtered) {
	var changes WALMessage
	if err := json.NewDecoder(bytes.NewReader(change)).Decode(&changes); err != nil {
		panic(fmt.Errorf("cant parse change from database to filter it %v", err))
	}

	if len(changes.Change) == 0 {
		return
	}

	for _, ch := range changes.Change {
		var filteredChanges = Wal2JsonChanges{
			Lsn:     lsn,
			Changes: []Wal2JsonChange{},
		}

		schema, exists := c.tables[utils.StreamIdentifier(ch.Table, ch.Schema)]
		if !exists {
			continue
		}

		builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		changesMap := map[string]interface{}{}
		if ch.Kind == "delete" {
			for i, changedValue := range ch.Oldkeys.Keyvalues {
				changesMap[ch.Oldkeys.Keynames[i]] = changedValue
			}
		} else {
			for i, changedValue := range ch.Columnvalues {
				changesMap[ch.Columnnames[i]] = changedValue
			}
		}

		for i, arrowField := range schema.Fields() {
			fieldName := arrowField.Name
			value := changesMap[fieldName]
			s := scalar.NewScalar(schema.Field(i).Type)
			if err := s.Set(value); err != nil {
				panic(fmt.Errorf("error setting value for column %s: %w", arrowField.Name, err))
			}

			scalar.AppendToBuilder(builder.Field(i), s)
		}

		filteredChanges.Changes = append(filteredChanges.Changes, Wal2JsonChange{
			Kind:   ch.Kind,
			Schema: ch.Schema,
			Table:  ch.Table,
			Row:    builder.NewRecord(),
		})

		OnFiltered(filteredChanges)
	}
}
