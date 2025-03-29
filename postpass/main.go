/*
 * "postpass"
 *
 * a simple wrapper around PostGIS that allows random people on the
 * internet to run PostGIS queries without ruining everything
 *
 * written by Frederik Ramm, GPL3+
 */

package main

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"time"
)

/* config stuff
 * should go into commandline arguments
 */
const (
	Host                 = "localhost"
	Port                 = 5432
	User                 = "readonly"
	Password             = "readonly"
	DBName               = "gis"
	QuickMediumThreshold = 100
	MediumSlowThreshold  = 50000
	ListenPort           = 8081
)

/*
 * API handler that receives a web request
 *
 * executes an EXPLAIN on the request
 * (which doubles as a syntax check)
 * and when EXPLAIN successful, sends the request to one of
 * three classes of worker.
 */
func handleApi(db *sql.DB, slow chan<- WorkItem, medium chan<- WorkItem, quick chan<- WorkItem, writer http.ResponseWriter, r *http.Request) {
	var res string

	// create channel we want to receive the response on
	rchan := make(chan SqlResponse)

	writer.Header().Set("Access-Control-Allow-Origin", "*")
	writer.Header().Set("Content-Type", "application/json")

	// process GET/POST parameters
	err := r.ParseForm()
	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	data := r.Form["data"][0]

	geojson := true
	tGeojson := r.Form["options[geojson]"]
	if tGeojson != nil {
		geojson, _ = strconv.ParseBool(tGeojson[0])
	}

	pretty := true
	tPretty := r.Form["options[pretty]"]
	if tPretty != nil {
		pretty, _ = strconv.ParseBool(tPretty[0])
	}

	collection := true
	tCollection := r.Form["options[collection]"]
	if tCollection != nil {
		collection, _ = strconv.ParseBool(tCollection[0])
	}

	// yes there is a possible SQL injection here but risk mitigation
	// must be done on PostgreSQL side - we do not want to build an SQL parser
	rows, err := db.Query(fmt.Sprintf("EXPLAIN (%s)", data))
	defer func(rows *sql.Rows) {
		_ = rows.Close()
	}(rows)

	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}

	// read only one row of EXPLAIN result
	rows.Next()

	// read only one column
	err = rows.Scan(&res)

	if err != nil {
		// in case EXPLAIN suddenly returns more columns
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}

	// parse cost from EXPLAIN result
	rx, _ := regexp.Compile("cost=(\\d+\\.\\d+)\\.\\.(\\d+\\.\\d+) rows")
	cost := rx.FindStringSubmatch(res)
	if len(cost) != 3 {
		http.Error(writer, "EXPLAIN response not parseable", http.StatusInternalServerError)
		return
	}

	// log.Printf("cost from %s to %s", cost[1], cost[2])
	from, err := strconv.ParseFloat(cost[1], 10)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
	to, err := strconv.ParseFloat(cost[2], 10)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}

	// use average of two cost values given by EXPLAIN
	med := (from + to) / 2

	// create work item...
	work := WorkItem{request: data, pretty: pretty, geojson: geojson, collection: collection, response: rchan}

	// ... and send to appropriate channel
	if med < QuickMediumThreshold {
		quick <- work
	} else if med < MediumSlowThreshold {
		medium <- work
	} else {
		slow <- work
	}

	// wait for response
	rv := <-rchan

	// and send response to HTTP client
	if rv.err {
		http.Error(writer, rv.result, http.StatusInternalServerError)
	} else {
		fmt.Fprintf(writer, "%s", rv.result)
	}
}

/*
 * main program
 */
func main() {

	// open a connection to the database
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable options='-c statement_timeout=1000000'",
		Host, Port, User, Password, DBName)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	db.SetMaxIdleConns(100)
	db.SetMaxOpenConns(200)
	db.SetConnMaxLifetime(time.Hour)

	// verify the connection
	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	// initialize goroutines
	quick_jobs := make(chan WorkItem, 50)
	for w := 1; w <= 10; w++ {
		go worker(db, 100+w, quick_jobs)
	}
	medium_jobs := make(chan WorkItem, 50)
	for w := 1; w <= 4; w++ {
		go worker(db, 200+w, medium_jobs)
	}
	slow_jobs := make(chan WorkItem, 50)
	for w := 1; w <= 2; w++ {
		go worker(db, 300+w, slow_jobs)
	}

	// set up callback for /interpreter URL
	http.HandleFunc("/interpreter", func(w http.ResponseWriter, r *http.Request) {
		handleApi(db, slow_jobs, medium_jobs, quick_jobs, w, r)
	})

	// endless loop
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", ListenPort), nil))

}
