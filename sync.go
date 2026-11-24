package strata

func (db *DB) syncFile() error {
	if db.syncHook != nil {
		return db.syncHook()
	}
	return syncData(db.data)
}
