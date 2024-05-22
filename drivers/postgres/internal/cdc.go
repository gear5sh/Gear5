package driver

import (
	"fmt"

	"github.com/piyushsingariya/shift/pkg/waljs"
	"github.com/piyushsingariya/shift/protocol"
	"github.com/piyushsingariya/shift/types"
)

func (p *Postgres) prepareWALJSConfig() (*waljs.Config, error) {
	if !p.Driver.GroupRead {
		return nil, fmt.Errorf("Invalid call; %s not running in CDC mode", p.Type())
	}

	config := &waljs.Config{
		Connection:          *p.config.Connection,
		ReplicationSlotName: p.cdcConfig.ReplicationSlot,
	}

	return config, nil
}

// Write Ahead Log Sync
func (p *Postgres) GroupRead(channel chan<- types.Record, streams ...protocol.Stream) error {
	config, err := p.prepareWALJSConfig()
	if err != nil {
		return err
	}

	return nil
}
