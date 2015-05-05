package goflat

// Connector mediates basic database functionality
type Connector interface {
	Connect(db, user string) (Session, error)
	Disconnect() error

	CreateView(name, query string) error
	CreateTrigger(name, def string, f TriggerFunc) error
}

// Connector mediates basic database functionality
type Session interface {
	// Runs provided function inside ACID transaction
	Transaction(func(Trx) error) error
	// Registers a view for use for all transactions created from this db connection
	//RegisterView(interface{}) error
	//RegisterFunc(interface{}) error
	/*GetView(string) *ViewParam*/
	//LastTransStats() stats
	//SetConcurrencyControl(concurrencyControl)
}

// Trx is a single ACID transaction descriptor
type Trx interface {
	// Inserts records into database
	Insert(...Set) error
	// Runs a sellect query from provided view
	Select(viewName string, args ...Value) ([]Set, error)
	//
	//Update(viewName string, args ...Value, ) error
}

type Key string
type Value interface{}
type Set map[Key]Value

