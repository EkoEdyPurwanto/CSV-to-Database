package database

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"studiKasusD.1/internal/variable"
)

func OpenDbConnection() (*sql.DB, error) {
	log.Println("=> open db connection")

	db, err := sql.Open("mysql", variable.DbConnString)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(variable.DbMaxConns)
	db.SetMaxIdleConns(variable.DbMaxIdleConns)

	return db, nil
}
