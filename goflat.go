// Package goflat contains a flat file NoSQL-like KV database with SQL-like DML syntax
package goflat

// Connector mediates basic database functionality
type Connector interface {
	// Connects to database with given credentials and returns new session
	Connect(db, user string) (Session, error)
	// Disconnect current session from database
	Disconnect() error
}

// Session descriptor
type Session interface {
	// Runs provided function inside ACID transaction
	Transaction(func(Trx) error) error
	Config() Config
	SetConfig(Config) error
	Stats() Stats
}

// Trx is a single ACID transaction descriptor
type Trx interface {
	// Initiates a new insert statement
	Insert() *InsertStmt
	// Initiates a new select statement
	Select(*Statement) *SelectStmt
	// Initiates a new update statement
	Update(*Statement) *UpdateStmt
	// Initiates a new delete statement
	Delete(*Statement) *DeleteStmt
}

// Set is a key:value data set
type Set map[Key]Value

// Key type is key in a key:value data set
type Key string

// Value type is a value in a key:value data set
type Value interface{}
