package table

type PrimaryKey interface {
	PK() string
	SK() interface{}
}
