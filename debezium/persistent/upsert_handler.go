package persistent

type UpsertHandler interface {
	Upsert(json string) error
}