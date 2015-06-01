package goflat

// Connector mediates basic database functionality
type Connector interface {
	Connect(db, user string) (Session, error)
	Disconnect() error
}

// Session descriptor
type Session interface {
	// Runs provided function inside ACID transaction
	Transaction(func(Trx) error) error
}

// Trx is a single ACID transaction descriptor
type Trx interface {
	// Initiates a new insert statement
	Insert() *insStatement
	// Initiates a new select statement
	Select() *selStatement
	// Initiates a new update statement
	Update() *updStatement
	// Initiates a new delete statement
	Delete() *delStatement
}

// A key:value data set
type Set map[Key]Value
// A key in a key:value data set
type Key string
// A value in a key:value data set
type Value interface{}
