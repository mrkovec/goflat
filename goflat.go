package goflat

// Connector mediates basic database functionality
type Connector interface {
	Connect(db, user string) (Session, error)
	Disconnect() error
 
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
	Insert() *insStatement
	// Runs a sellect query from provided view
	Select() *selStatement
	Update() *updStatement
	Delete() *delStatement 
	//Select(viewName string, args ...Value) ([]Set, error)
	// Updates records selected by view to 
	//Update(viewName string, args ...Value, set Set) (int, error)
}

type Key string
type Value interface{}
type Set map[Key]Value

