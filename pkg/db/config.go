package db

import "fmt"

// DBConn represents database connection parameters
type DBConn struct {
	// Driver is the db driver (e.g. postgres)
	Driver string
	// Host is the host address where the db reside
	Host string
	// Port is the port on which the db is listening for connections
	Port string
	// Username is the username for authc/z against the db
	Username string
	// Password is the password for authc/z against the db
	Password string
	// DBName is the name of the db to connect to
	DBName string
	// SSLMode is the SSL mode to use
	SSLMode string
	// MaxConnections is the max number of open connections to db
	MaxConnections *int
	// MaxIdleConnections is the max number of idle connections to db
	MaxIdleConnections *int
	// MaxConnIdleTimeSec is the duration (in seconds) while idle connections will be alive and available for reusing
	MaxConnIdleTimeSec *int
}

// SourceName returns the DSN for the connection
func (d *DBConn) SourceName() string {
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		d.Host, d.Port, d.Username, d.Password, d.DBName, d.SSLMode)
}

// URL returns the URL for the connection
func (d *DBConn) URL() string {
	return fmt.Sprintf("%s://%s:%s@%s:%s/%s?sslmode=%s",
		d.Driver, d.Username, d.Password, d.Host, d.Port, d.DBName, d.SSLMode)
}
