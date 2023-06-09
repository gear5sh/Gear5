package driver

import syndicatemodels "github.com/piyushsingariya/syndicate/models"

type HubspotStream interface {
	Read(channel <-chan syndicatemodels.Record) error
	Stream() *syndicatemodels.Stream
}
