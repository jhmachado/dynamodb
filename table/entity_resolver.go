package table

type EntityResolver interface {
	CreateZeroEntity(primaryKey PrimaryKey) (interface{}, bool, error)
	JoinEntities(topEntity, relatedEntity interface{}, sk interface{}) error
}
