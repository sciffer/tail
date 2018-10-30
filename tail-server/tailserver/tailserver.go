package ctailserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings" //"github.com/Shopify/sarama"

	cluster "github.com/bsm/sarama-cluster"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sciffer/sse"
)

type ctailserver struct {
	ingested, errors prometheus.CounterVec
	logger           log.Logger
	server           sse.Server
	mux              http.ServeMux
	services         []string
	verbose          bool
	bufferSize       int
	consumer         cluster.Consumer
}

func (s *ctailserver) StartHTTP(uri string, listen string) {
	s.mux.HandleFunc(uri, s.server.HTTPHandler)
	s.mux.HandleFunc("/test", func(w http.ResponseWriter, _ *http.Request) { fmt.Fprintf(w, "OK") })
	s.mux.HandleFunc("/services", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("content-type") == "application/json" {
			servicesjson, _ := json.Marshal(s.services)
			fmt.Fprintf(w, string(servicesjson))
		} else {
			fmt.Fprintf(w, strings.Join(s.services, "\n"))
		}
	})
	s.mux.Handle("/metrics", promhttp.Handler())
	go http.ListenAndServe(listen, &s.mux)
	s.logger.Println("Listener started")
}

func (s *ctailserver) InitConsumer(brokerList string, topic string, group string) {
	if s.verbose {
		//sarama.Logger = logger - To be replaced with sarma-cluster logger
	}
	conf := cluster.NewConfig()
	/*switch *offset {
	case "oldest":
		conf.Consumer.Offsets.Initial = sarama.OffsetOldest
	case "newest":
		conf.Consumer.Offsets.Initial = sarama.OffsetNewest
	default:
		printUsageErrorAndExit("-offset should be `oldest` or `newest`")
	}*/

	consumer, err := cluster.NewConsumer(strings.Split(brokerList, ","), group, []string{topic}, conf)
	if err != nil {
		s.logger.Fatalf("Failed to open consumer: %s", err)
		printErrorAndExit(69, "Failed to open consumer: %s", err)
	}
	s.consumer = *consumer
}

func (s *ctailserver) StartConsuming() {
	defer s.consumer.Close()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Kill, os.Interrupt)

	s.logger.Println("Starting consumers and queue processing...")

	service := ""
	owner := ""
	etype := "event"
	for {
		select {
		case msg, more := <-s.consumer.Messages():
			if more {
				service = ExtractString("app", msg.Value)
				owner = ExtractString("owner", msg.Value)
				if owner == "" {
					owner = ExtractString("obowner", msg.Value)
					if owner == "" {
						owner = "none"
					}
				}
				if service != "" {
					s.server.Publish(service, msg.Value)
					if !StringExists(service, s.services) {
						s.services = append(s.services, service)
						if s.verbose {
							s.logger.Printf("Registered new service '%s' found in message: '%s'", service, msg.Value)
						}
					}
					etype = "event"
					if LocateString("EVENT", msg.Value) == -1 {
						etype = "log"
					}
					s.ingested.With(prometheus.Labels{"service": service, "owner": owner, "type": etype}).Inc()
				} else {
					s.server.Publish("none", msg.Value)
					s.ingested.With(prometheus.Labels{"service": "none", "owner": owner, "type": etype}).Inc()
				}
				s.consumer.MarkOffset(msg, "") // mark message as processed
			}
		case err, more := <-s.consumer.Errors():
			if more {
				s.logger.Printf("Error: %s\n", err.Error())
				s.errors.With(prometheus.Labels{"error": "kafka_consumption"}).Inc()
			}
		case ntf, more := <-s.consumer.Notifications():
			if more {
				s.logger.Printf("Rebalanced: %+v\n", ntf)
				s.errors.With(prometheus.Labels{"error": "kafka_rebalance"}).Inc()
			}
		case <-signals:
			s.logger.Println("Done consuming")
			return
		}
	}
}

func NewCtailServer(verbose bool, bufferSize int) ctailserver {
	server := ctailserver{}
	server.ingested = *prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ctail_ingested_logs",
		Help: "Number of ingested logs by this ctail server since startup(per service).",
	},
		[]string{"service", "owner", "type"})
	server.errors = *prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ctail_errors",
		Help: "Number of ctail errors per error type.",
	},
		[]string{"error"})
	prometheus.MustRegister(server.ingested)
	prometheus.MustRegister(server.errors)
	server.errors.With(prometheus.Labels{"error": "kafka_consumption"}).Add(0)
	server.errors.With(prometheus.Labels{"error": "kafka_rebalance"}).Add(0)
	server.logger = *log.New(os.Stderr, "", log.LstdFlags)
	server.server = *sse.New()
	server.mux = *http.NewServeMux()
	server.services = []string{}
	server.bufferSize = bufferSize
	server.verbose = verbose

	return server
}

func LocateString(field string, byteArr []byte) int {
	return bytes.Index(byteArr, []byte(field))
}

func ExtractString(field string, byteArr []byte) string {
	field = "\"" + field + "\":\""
	ind := bytes.Index(byteArr, []byte(field))
	if ind == -1 {
		return ""
	}
	ind += len(field)
	to := bytes.IndexByte(byteArr[ind:], byte('"'))
	return string(byteArr[ind:(ind + to)])
}

func StringExists(s string, slice []string) bool {
	for _, val := range slice {
		if s == val {
			return true
		}
	}
	return false
}

func printErrorAndExit(code int, format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "ERROR: %s\n", fmt.Sprintf(format, values...))
	fmt.Fprintln(os.Stderr)
	os.Exit(code)
}
