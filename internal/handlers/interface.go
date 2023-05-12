package handlers

import (
	"database/sql"
	"encoding/csv"
	"os"
	"sync"
)

type StudiKasusD1 interface {
	OpenCsvFile() (*csv.Reader, *os.File, error)
	DispatchWorkers(db *sql.DB, jobs <-chan []interface{}, wg *sync.WaitGroup)
	ReadCsvFilePerLineThenSendToWorker(csvReader *csv.Reader, jobs chan<- []interface{}, wg *sync.WaitGroup)
	DoTheJob(workerIndex, counter int, db *sql.DB, values []interface{})
	GenerateQuestionsMark(n int) []string
}
