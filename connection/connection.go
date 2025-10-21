package connection

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"sync"

	"github.com/Nemutagk/godb/v2/definitions/adapter"
	"github.com/Nemutagk/godb/v2/definitions/config"
)

type PostgresAdapter struct {
	Name string
	*adapter.Config
	mu sync.Mutex
}

func NewConnection() (*PostgresAdapter, error) {
	return &PostgresAdapter{
		Name:   "",
		Config: &adapter.Config{},
	}, nil
}

func (p *PostgresAdapter) SetConf(name string, conf config.Config) error {
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		conf.Host, conf.Port, conf.User, conf.Password, conf.Database)

	p.Dsn = dsn
	p.Name = name

	return nil
}

func (p *PostgresAdapter) Connect() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.Conn != nil {
		return nil
	}

	if p.Dsn == "" {
		return fmt.Errorf("DSN is not set")
	}

	var connErr error
	conn, connErr := sql.Open("postgres", p.Dsn)
	if connErr != nil {
		return fmt.Errorf("error opening database connection: %w", connErr)
	}

	if err := conn.Ping(); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	log.Println("Connecting to Postgres database for '" + p.Name + "'")
	p.Conn = conn

	return nil
}

func (p *PostgresAdapter) GetConnection() any {
	return p.Conn
}

func (p *PostgresAdapter) Close() error {
	if p.Conn != nil {
		if closer, ok := p.Conn.(io.Closer); ok {
			return closer.Close()
		}
	}

	return nil
}

func (p *PostgresAdapter) Ping() error {
	if p.Conn == nil {
		return fmt.Errorf("no active database connection")
	}

	return p.Conn.(*sql.DB).Ping()
}
