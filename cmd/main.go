package main

import (
	"fmt"
	"log"
	"math"
	"studiKasusD.1/internal/database"
	"studiKasusD.1/internal/handlers"
	"sync"
	"time"
)

func main() {
	start := time.Now()

	db, err := database.OpenDbConnection()
	if err != nil {
		log.Fatal(err.Error())
	}

	skImpl := handlers.NewStudiKasusD1Impl(db)
	csvReader, csvFile, err := skImpl.OpenCsvFile()
	if err != nil {
		log.Fatal(err.Error())
	}
	defer csvFile.Close()

	jobs := make(chan []interface{}, 0)
	wg := new(sync.WaitGroup)

	go skImpl.DispatchWorkers(db, jobs, wg)
	skImpl.ReadCsvFilePerLineThenSendToWorker(csvReader, jobs, wg)

	wg.Wait()

	duration := time.Since(start)
	fmt.Println("done in", int(math.Ceil(duration.Seconds())), "seconds")
}
