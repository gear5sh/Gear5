package jdbc

import "github.com/piyushsingariya/shift/types"

const CDCDeletedAt = "_cdc_deleted_at"
const CDCLSN = "_cdc_lsn"
const CDCUpdatedAt = "_cdc_updated_at"

var CDCColumns = map[string]types.DataType{
	CDCDeletedAt: types.TIMESTAMP,
	CDCLSN:       types.STRING,
	CDCUpdatedAt: types.TIMESTAMP,
}
