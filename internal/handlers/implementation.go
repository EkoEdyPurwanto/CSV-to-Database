package handlers

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"studiKasusD.1/internal/variable"
	"sync"
)

type StudiKasusD1Impl struct {
	DB *sql.DB
}

func NewStudiKasusD1Impl(DB *sql.DB) *StudiKasusD1Impl {
	return &StudiKasusD1Impl{DB: DB}
}

func (s *StudiKasusD1Impl) OpenCsvFile() (*csv.Reader, *os.File, error) {
	log.Println("=> open csv file")

	f, err := os.Open(variable.CsvFile)
	if err != nil {
		return nil, nil, err
	}

	reader := csv.NewReader(f)
	return reader, f, nil
}

func (s *StudiKasusD1Impl) DispatchWorkers(db *sql.DB, jobs <-chan []interface{}, wg *sync.WaitGroup) {
	for workerIndex := 0; workerIndex <= variable.TotalWorker; workerIndex++ {
		go func(workerIndex int, db *sql.DB, jobs <-chan []interface{}, wg *sync.WaitGroup) {
			counter := 0

			for job := range jobs {
				s.DoTheJob(workerIndex, counter, db, job)
				wg.Done()
				counter++
			}
		}(workerIndex, db, jobs, wg)
	}
}

func (s *StudiKasusD1Impl) ReadCsvFilePerLineThenSendToWorker(csvReader *csv.Reader, jobs chan<- []interface{}, wg *sync.WaitGroup) {
	for {
		row, err := csvReader.Read()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			break
		}

		if len(variable.DataHeaders) == 0 {
			variable.DataHeaders = row
			continue
		}

		rowOrdered := make([]interface{}, 0)
		for _, each := range row {
			rowOrdered = append(rowOrdered, each)
		}

		wg.Add(1)
		jobs <- rowOrdered
	}
	close(jobs)
}

func (s *StudiKasusD1Impl) DoTheJob(workerIndex, counter int, db *sql.DB, values []interface{}) {
	for {
		var outerError error
		func(outerError *error) {
			defer func() {
				if err := recover(); err != nil {
					*outerError = fmt.Errorf("%v", err)
				}
			}()

			conn, err := db.Conn(context.Background())
			query := fmt.Sprintf("INSERT INTO domain (%s) VALUES (%s)",
				strings.Join(variable.DataHeaders, ","),
				strings.Join(s.GenerateQuestionsMark(len(variable.DataHeaders)), ","),
			)

			_, err = conn.ExecContext(context.Background(), query, values...)
			if err != nil {
				log.Fatal(err.Error())
			}

			err = conn.Close()
			if err != nil {
				log.Fatal(err.Error())
			}
		}(&outerError)
		if outerError == nil {
			break
		}
	}

	if counter%100 == 0 {
		log.Println("=> worker", workerIndex, "inserted", counter, "data")
	}
}

func (s *StudiKasusD1Impl) GenerateQuestionsMark(n int) []string {
	w := make([]string, 0)
	for i := 0; i < n; i++ {
		w = append(w, "?")
	}
	return w
}
