package util

type MemoryOnlyPrimaryKey struct {
	Pk string
	Sk interface{}
}

func (p MemoryOnlyPrimaryKey) PK() string {
	return p.Pk
}

func (p MemoryOnlyPrimaryKey) SK() interface{} {
	return p.Sk
}
