// Fetch ElasticSearch cluster status and performance data and store it
// into Graphite.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"log/syslog"
	"net"
	"net/http"
	"time"
)

import _ "expvar"

// DefaultTimeout is connection timeout, in seconds
const DefaultTimeout = 5

var (
	elasticHost  = flag.String("eh", "", "IP, FQDN or hostname of your ElasticSearch host")
	elasticPort  = flag.Int("ep", 9200, "ElasticSearch hosts port")
	graphiteHost = flag.String("gh", "", "IP, FQDN or hostname of your Carbon host")
	graphitePort = flag.Int("gp", 2003, "Carbon hosts port")
	graphiteDb   = flag.String("gd", "elasticsearch.cluster", "Graphite database name")
	pollPeriod   = flag.Duration("poll", 20*time.Second, "Metrics poll interval")
)

var (
	// elasticData is JSON response from ElasticSearch host
	elasticData chan interface{}

	// graphiteData is transformed data into valid Carbon metrics
	graphiteData chan string
)

// Server describes how service should be configured
type Server struct {
	// Shared syslog between methods for this receiver
	Syslog *syslog.Writer

	// Poll interval for fetching ES cluster data and storing it into Graphite
	period time.Duration

	// Stats URL of ES node
	elasticURL string

	// Carbon host
	graphite string

	// Graphite database name
	graphiteDb string

	// Connection timeout
	timeout time.Duration
}

func init() {
	flag.Parse()
}

func main() {
	elasticData = make(chan interface{})
	graphiteData = make(chan string)

	s := NewServer()
	s.run()
}

func NewServer() *Server {
	s := &Server{
		elasticURL: fmt.Sprintf("http://%s:%d/_cluster/health", *elasticHost, *elasticPort),
		graphite:   fmt.Sprintf("%s:%d", *graphiteHost, *graphitePort),
		graphiteDb: *graphiteDb,
		period:     *pollPeriod,
		timeout:    DefaultTimeout * time.Second,
	}

	s.setupSyslog()
	fmt.Printf("ElasticSearch cluster health URL: %s\n", s.elasticURL)
	fmt.Printf("Poll period: %s\n", s.period)

	return s
}

// Setups syslog
func (s *Server) setupSyslog() {
	var err error
	s.Syslog, err = syslog.New(syslog.LOG_ERR, "esmetrics")
	defer s.Syslog.Close()
	if err != nil {
		log.Fatal("Could not establish connection to the system log daemon")
	}
}

// Run our service
func (s *Server) run() {
	for {
		go s.fetchElasticData()
		data := <-elasticData

		if data != nil {
			go s.createGraphiteData(data)
			go s.sendToGraphite(<-graphiteData)
		}
		time.Sleep(s.period)
	}
}

// Fetches data from ElasticSearch host and sends it
// to a channel. If there is a error nil will be sent to
// channel.
func (s *Server) fetchElasticData() {
	var (
		resp     *http.Response
		body     []byte
		jsonData interface{}
		err      error
	)

	resp, err = http.Get(s.elasticURL)
	if err != nil {
		s.Syslog.Err("Could not connect to ElasticSearch server")
		elasticData <- nil
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		s.Syslog.Warning("HTTP response code was not 200: " + resp.Status)
		elasticData <- nil
		return
	}

	body, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		s.Syslog.Warning(err.Error())
		elasticData <- nil
		return
	}

	jsonData, err = decodeJSON(body)
	if err != nil {
		s.Syslog.Warning("Could not decode JSON data: " + err.Error())
		elasticData <- nil
		return
	}

	elasticData <- jsonData
}

// Creates valid Carbon metrics, in following format:
// "<graphite_database >.<metric> <value> <timestamp> \n"
func (s *Server) createGraphiteData(data interface{}) {
	t := time.Now()
	var tmp string

	for k, v := range data.(map[string]interface{}) {
		if k == "cluster_name" {
			v = 0
		} else if k == "timed_out" {
			if v == true {
				v = 1
			} else {
				v = 0
			}
		} else if k == "cluster_status" || k == "status" {
			if v == "green" {
				v = 0
			}

			if v == "yellow" {
				v = 1
			}

			if v == "red" {
				v = 2
			}
		}

		tmp += fmt.Sprintf("%s.%s %v %d\n", s.graphiteDb, k, v, t.Unix())
	}

	graphiteData <- tmp
}

// Connects to Carbon host and sends data to it
func (s *Server) sendToGraphite(data string) {
	var (
		conn net.Conn
		err  error
	)

	conn, err = net.DialTimeout("tcp", s.graphite, s.timeout)
	if err != nil {
		s.Syslog.Warning("Could not connect to Carbon server")
		return
	}
	defer conn.Close()

	buf := bytes.NewBufferString(data)
	_, err = conn.Write(buf.Bytes())
	if err != nil {
		s.Syslog.Warning("Could not send data to Carbon server: " + err.Error())
	}

}

// Decodes JSON data
func decodeJSON(jsonBlob []byte) (interface{}, error) {
	var r interface{}

	err := json.Unmarshal(jsonBlob, &r)
	if err != nil {
		return nil, err
	}

	return r, nil
}
