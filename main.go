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

const defaultTimeout = 5

var (
	elasticHost  = flag.String("eh", "", "IP, FQDN or hostname of your ElasticSearch host")
	elasticPort  = flag.Int("ep", 9200, "ElasticSearch hosts port")
	graphiteHost = flag.String("gh", "", "IP, FQDN or hostname of your Carbon host")
	graphitePort = flag.Int("gp", 2003, "Carbon hosts port")
	graphiteDb   = flag.String("gd", "elasticsearch.cluster", "Graphite database name")
	pollPeriod   = flag.Duration("poll", 20*time.Second, "Metrics poll interval")
)

var (
	elasticData  chan interface{}
	graphiteData chan string
)

type Server struct {
	Syslog     *syslog.Writer
	period     time.Duration
	elasticUrl string
	graphite   string
	graphiteDb string
	timeout    time.Duration
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
		elasticUrl: fmt.Sprintf("http://%s:%d/_cluster/health", *elasticHost, *elasticPort),
		graphite:   fmt.Sprintf("%s:%d", *graphiteHost, *graphitePort),
		graphiteDb: *graphiteDb,
		period:     *pollPeriod,
		timeout:    defaultTimeout * time.Second,
	}

	s.setupSyslog()
	fmt.Printf("ElasticSearch cluster health URL: %s\n", s.elasticUrl)
	fmt.Printf("Poll period: %s\n", s.period)

	return s
}

func (s *Server) setupSyslog() {
	var err error
	s.Syslog, err = syslog.New(syslog.LOG_ERR, "esmetrics")
	defer s.Syslog.Close()
	if err != nil {
		log.Fatal("Could not establish connection to the system log daemon")
	}
}

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

func (s *Server) fetchElasticData() {
	var (
		resp     *http.Response
		body     []byte
		jsonData interface{}
		err      error
	)

	resp, err = http.Get(s.elasticUrl)
	if err != nil {
		s.Syslog.Err("Could not connect to ElasticSearch server")
		elasticData <- nil
		return
	} else {
		defer resp.Body.Close()
	}

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

func (s *Server) sendToGraphite(data string) {
	var (
		conn net.Conn
		err  error
	)

	conn, err = net.DialTimeout("tcp", s.graphite, s.timeout)
	if err != nil {
		s.Syslog.Warning("Could not connect to Carbon server")
		return
	} else {
		defer conn.Close()
	}

	buf := bytes.NewBufferString(data)
	_, err = conn.Write(buf.Bytes())
	if err != nil {
		s.Syslog.Warning("Could not send data to Carbon server: " + err.Error())
	}

}

func decodeJSON(jsonBlob []byte) (interface{}, error) {
	var r interface{}

	err := json.Unmarshal(jsonBlob, &r)
	if err != nil {
		return nil, err
	}

	return r, nil
}
