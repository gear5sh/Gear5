package driver

import (
	"time"

	"github.com/piyushsingariya/syndicate/logger"
	"github.com/piyushsingariya/syndicate/types"
	"github.com/piyushsingariya/syndicate/utils"
)

type IncrementalStream struct {
	Stream
	statePk string

	// limit = 1000
	// # Flag which enable/disable chunked read in read_chunked method
	// # False -> chunk size is max (only one slice), True -> chunk_size is 30 days
	// need_chunk = True
	// state_checkpoint_interval = 500
	// last_slice = None
	limit                   int
	stateCheckpointInterval int
	lastSlice               any
	needChunk               bool

	_state    *time.Time
	_mode     *types.SyncMode
	_initSync time.Time
}

func newIncrementalStream(stream Stream, statePK string) *IncrementalStream {
	s := &IncrementalStream{
		Stream:                  stream,
		limit:                   1000,
		stateCheckpointInterval: 500,
		needChunk:               true,
		_initSync:               time.Now(),
		statePk:                 statePK,
	}

	// populating defaults
	if s.statePk == "" {
		s.statePk = "timestamp"
	}

	return s
}

func (i *IncrementalStream) cursorField() string {
	return i.updatedAtField
}

func (i *IncrementalStream) state() map[string]any {
	if i._mode == nil {
		logger.Fatalf("sync_mode is not defined for stream %s", i.Name())
	}

	if i._state != nil {
		if i.statePk == "timestamp" {
			return map[string]any{i.cursorField(): i._state.Unix() * 1000}
		}
		return map[string]any{i.cursorField(): i._state.String()}
	}

	return nil
}

func (i *IncrementalStream) setup(mode types.SyncMode, state map[string]any) {
	i._mode = &mode
	if state != nil {
		if value, found := state[i.cursorField()]; found {
			date, err := utils.ReformatDate(value)
			if err != nil {
				logger.Fatalf("failed to reformate date in state map: %v : %s", value, err)
			}

			i._state = &date
		}
	}
}

func (i *IncrementalStream) updateState(latestCursor time.Time, isLastRecord bool) {
	// The first run uses an endpoint that is not sorted by updated_at but is
	// sorted by id because of this instead of updating the state by reading
	// the latest cursor the state will set it at the end with the time the synch
	// started. With the proposed `state strategy`, it would capture all possible
	// updated entities in incremental synch.
	newState := time.Time{}
	if i._state != nil {
		newState = utils.MaxDate(*i._state, latestCursor)
	} else {
		newState = latestCursor
	}

	if newState != *i._state {
		logger.Infof("Advancing bookmark for %s stream from %s to %s", i.Name(), i._state.GoString(), newState.GoString())
		i._state = &newState
		i.startDate = *i._state
	}

	if isLastRecord {
		i._state = &i._initSync
	}
}

func (i *IncrementalStream) readRecords(mode types.SyncMode, cursorField []string) {

}

func (i *IncrementalStream) streamSlices(mode)