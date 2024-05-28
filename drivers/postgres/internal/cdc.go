package driver

import (
	"fmt"

	"github.com/piyushsingariya/shift/drivers/base"
	"github.com/piyushsingariya/shift/pkg/jdbc"
	"github.com/piyushsingariya/shift/pkg/waljs"
	"github.com/piyushsingariya/shift/protocol"
	"github.com/piyushsingariya/shift/safego"
	"github.com/piyushsingariya/shift/types"
)

func (p *Postgres) prepareWALJSConfig(streams ...protocol.Stream) (*waljs.Config, error) {
	if !p.Driver.GroupRead {
		return nil, fmt.Errorf("Invalid call; %s not running in CDC mode", p.Type())
	}

	config := &waljs.Config{
		Connection:          *p.config.Connection,
		ReplicationSlotName: p.cdcConfig.ReplicationSlot,
		InitialWaitTime:     p.cdcConfig.InitialWaitTime,
		State:               p.cdcState,
		FullSyncTables:      types.NewSet[protocol.Stream](),
		Tables:              types.NewSet[protocol.Stream](),
	}

	for _, stream := range streams {
		if stream.GetState() == nil {
			config.FullSyncTables.Insert(stream)
		}

		config.Tables.Insert(stream)
	}

	return config, nil
}

func (p *Postgres) StateType() types.StateType {
	return types.MixedType
}

// func (p *Postgres) GlobalState() any {
// 	return p.cdcState
// }

func (p *Postgres) SetupGlobalState(state *types.State) error {
	state.Type = p.StateType()
	// Setup raw state
	p.cdcState = types.NewGlobalState(&waljs.WALState{})

	return base.ManageGlobalState(state, p.cdcState, p)
}

// Write Ahead Log Sync
func (p *Postgres) GroupRead(channel chan<- types.Record, streams ...protocol.Stream) error {
	config, err := p.prepareWALJSConfig(streams...)
	if err != nil {
		return err
	}

	socket, err := waljs.NewConnection(config)
	if err != nil {
		return err
	}

	return socket.OnMessage(func(message waljs.WalJSChange) bool {
		if message.Kind == "delete" {
			message.Data[jdbc.CDCDeletedAt] = message.Timestamp
		}
		if message.Timestamp != nil {
			message.Data[jdbc.CDCUpdatedAt] = message.Timestamp
		}
		if message.LSN != nil {
			message.Data[jdbc.CDCLSN] = message.LSN
		}

		// insert record
		if !safego.Insert(channel, base.ReformatRecord(message.Stream, message.Data)) {
			// channel was closed; exit OnMessage
			return true
		}

		return false
	})
}
