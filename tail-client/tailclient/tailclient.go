package ctailclient

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"text/template"
	"time"
	"github.com/sciffer/tail/tail-client/domain"
	"github.com/sciffer/tail/tail-client/elasticsearch"
	ctemplate "github.com/sciffer/tail/tail-client/template"
	"github.com/hokaccha/go-prettyjson"
	"github.com/sciffer/sse"
)

type ctailclient struct {
	tmpl                                                       *template.Template
	logger                                                     log.Logger
	podsfilter, clustersfilter, esclusters, esindices, urllist []string
	esfilters                                                  map[string]interface{}
	service, env, podid, rev, uri, timeOffset                  string
	location                                                   *time.Location
	history                                                    bool
	bufferSize, maxMessages                                    int
	messages                                                   chan *sse.Event
	clients                                                    []*sse.Client
	done                                                       chan bool
}

// NewCtailClient Create new ctailclient object and initialize basic attributes
func NewCtailClient(servers string, uri string, service string, fieldsArg string, history bool, timezone string, bufferSize int) ctailclient {
	client := ctailclient{}
	client.esfilters = make(map[string]interface{})
	client.logger = *log.New(os.Stderr, "", log.LstdFlags)
	client.service = service
	client.uri = uri
	client.history = history
	client.bufferSize = bufferSize
	client.urllist = getUrls(servers)

	tmpl, err := ctemplate.CreateTemplate(strings.Split(fieldsArg, ","))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error with fields %s\nuse -show-fields to display valid fields\n", err)
		os.Exit(-2)
	}
	client.tmpl = tmpl

	location, err := time.LoadLocation(timezone)
	if err != nil {
		printUsageErrorAndExit("provided timestamp value seems to be invalid, please stick to IANA timestamp format.")
	}
	client.location = location
	// Line added by Doug
	domain.Location = location

	// ctailclient parallelism channels
	client.messages = make(chan *sse.Event, client.bufferSize) // The channel will be used by all clients as a destination to events.
	client.clients = []*sse.Client{}                           //slice of the sse clients, client per endpoint

	return client
}

// SetFilters sets ctailclient filter attributes
func (c *ctailclient) SetFilters(pods string, clusters string, podid string, env string, rev string) {
	// basic filters
	c.env = env
	c.podid = podid
	c.rev = rev
	// build pod filters
	if pods != "" {
		if c.history {
			c.esfilters["kubernetes.pod_name.keyword"] = strings.Split(pods, ",")
		} else {
			c.podsfilter = strings.Split(pods, ",")
		}
	}

	// build cluster filters
	if clusters != "" {
		if c.history {
			c.esfilters["kubernetes.labels.kubeCluster.keyword"] = strings.Split(clusters, ",")
		} else {
			c.clustersfilter = strings.Split(clusters, ",")
		}
	}
}

// SetHistoryParams sets ctailclient history parameters
func (c *ctailclient) SetHistoryParams(elasticClusters string, indices string, maxMessages int, timeOffset string) {
	c.esclusters = strings.Split(elasticClusters, ",")
	if len(c.env) > 0 {
		c.esfilters["kubernetes.labels.environment.keyword"] = c.env
		c.env = ""
	}
	if len(c.rev) > 0 {
		c.esfilters["kubernetes.labels.version.keyword"] = c.rev
		c.rev = ""
	}
	if len(c.podid) > 0 {
		c.esfilters["kubernetes.pod_id.keyword"] = c.podid
		c.podid = ""
	}
	c.esindices = strings.Split(indices, ",")
	c.timeOffset = timeOffset
	c.maxMessages = maxMessages

	// Create a Done channel for history - to know when queries ended
	c.done = make(chan bool)
}

func (c *ctailclient) Subscribe2Elasticsearch() {
	c.logger.Print("Initializing clients:")
	// Issue parallel elasticsearch queries against all clusters
	for _, escluster := range c.esclusters {
		client := elasticsearch.NewElasticsearch(escluster, c.esindices, c.service, c.timeOffset)
		subscribeReport := func() {
			if err := client.Query2sse(c.messages, c.esfilters, c.maxMessages); err != nil {
				fmt.Println(err)
			}
			c.done <- true
		}
		go subscribeReport()
		//c.logger.Print(".")
	}
	// Closer function - waits for queries to finish + waits secToWaitBeforeExit before closing the messages channel
	go func() {
		counter := 0
		for counter < len(c.esclusters) && <-c.done {
			counter++
		}
		close(c.messages)
	}()
	c.logger.Println("Done")
}

func (c *ctailclient) Subscribe2CtailServers() {
	c.logger.Print("Initializing clients:")
	for _, url := range c.urllist {
		serverservices := c.GetServices([]string{url})
		if includes(serverservices, c.service) {
			client := sse.NewClient(url + c.uri)
			subscribeReport := func() {
				if err := client.SubscribeChan(c.service, c.messages); err != nil {
					c.logger.Println(err)
				}
			}
			go subscribeReport()
			c.clients = append(c.clients, client)
			//fmt.Print(".")
		}
	}
	c.logger.Println("Done")
}

// ConsumeAndPrint consumes the logs/events and prints the output
func (c *ctailclient) ConsumeAndPrint(isEvents bool, pretty bool, msgOnly bool) {
	if len(c.clients) > 0 || c.history {
		c.logger.Println("Waiting for log messages to arrive:")
		for msg := range c.messages {
			var jsonmsg map[string]interface{}
			json.Unmarshal(msg.Data, &jsonmsg)

			// Filter out nil and filters based on parameters
			if jsonmsg == nil || (!c.history && isEvents != IsEvent(&jsonmsg)) || !c.filterEvent(&jsonmsg) {
				continue
			}

			// Align timestamp to local
			if timestamp, isTimestamp := jsonmsg["@timestamp"]; isTimestamp {
				if parsed, err := time.Parse(time.RFC3339, timestamp.(string)); err == nil {
					jsonmsg["@timestamp"] = parsed.In(c.location)
				}
			} else {
				jsonmsg["@timestamp"] = time.Now().In(c.location)
			}

			if pretty {
				prettyMessagePrint(jsonmsg, msgOnly, isEvents)
			} else {
				messagePrint(jsonmsg, msgOnly, isEvents, c.tmpl)
			}
		}
		c.logger.Println("Closing client subscriptions...")
	} else {
		c.logger.Println("No clients initialized, so no messages will be recieved - closing.")
	}
}

// FilterEvent returns true if event matches provided filters
func (c *ctailclient) filterEvent(jsonmsg *map[string]interface{}) bool {
	// Filter of pod_name
	if len(c.podsfilter) > 0 {
		matched := false
		if _, isKube := (*jsonmsg)["kubernetes"]; isKube {
			if val, isPodName := ((*jsonmsg)["kubernetes"].(map[string]interface{}))["pod_name"]; isPodName {
				if includes(c.podsfilter, val.(string)) {
					matched = true
				}
			}
		}
		if !matched {
			return false
		}
	}
	// Filter of Environment
	if len(c.env) > 0 {
		matched := false
		if _, isKube := (*jsonmsg)["kubernetes"]; isKube {
			if _, isLabels := ((*jsonmsg)["kubernetes"].(map[string]interface{}))["labels"]; isLabels {
				if val, isEnvironment := (((*jsonmsg)["kubernetes"].(map[string]interface{}))["labels"].(map[string]interface{}))["environment"].(string); isEnvironment && c.env == val {
					matched = true
				}
			}
		}
		if !matched {
			return false
		}
	}
	// Filter of Revision/Version
	if len(c.rev) > 0 {
		matched := false
		if _, isKube := (*jsonmsg)["kubernetes"]; isKube {
			if _, isLabels := ((*jsonmsg)["kubernetes"].(map[string]interface{}))["labels"]; isLabels {
				if val, isVersion := (((*jsonmsg)["kubernetes"].(map[string]interface{}))["labels"].(map[string]interface{}))["version"].(string); isVersion && c.rev == val {
					matched = true
				}
			}
		}
		if !matched {
			return false
		}
	}
	// Filter of Pod_ID
	if len(c.podid) > 0 {
		matched := false
		if _, isKube := (*jsonmsg)["kubernetes"]; isKube {
			if val, isPodID := ((*jsonmsg)["kubernetes"].(map[string]interface{}))["pod_id"].(string); isPodID && c.podid == val {
				matched = true
			}
		}
		if !matched {
			return false
		}
	}
	// Filter of KubeCluster
	if len(c.clustersfilter) > 0 {
		matched := false
		if _, isKube := (*jsonmsg)["kubernetes"]; isKube {
			if _, isLabels := ((*jsonmsg)["kubernetes"].(map[string]interface{}))["labels"]; isLabels {
				if val, isKubeCluster := (((*jsonmsg)["kubernetes"].(map[string]interface{}))["labels"].(map[string]interface{}))["kubeCluster"].(string); isKubeCluster {
					if includes(c.clustersfilter, val) {
						matched = true
					}
				}
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

// IsEvent returns true if message is Event
func IsEvent(jsonmsg *map[string]interface{}) bool {
	if tags, ok := (*jsonmsg)["tags"]; ok {
		return strInterfaceIncluded(tags.([]interface{}), "EVENT")
	}
	return false
}

// strInterfaceIncluded returns true if string interface included in string list/slice
func strInterfaceIncluded(values []interface{}, value2match string) bool {
	for _, val := range values {
		if val.(string) == value2match {
			return true
		}
	}
	return false
}

// GetServices returns a list of services available from ctailservers
func (c *ctailclient) GetServices(urls []string) []string {
	if len(urls) == 0 {
		urls = c.urllist
	}
	services := []string{}
	DefaultClient := http.Client{}

	c.logger.Printf("Connecting to: %s\n", strings.Join(urls, ","))
	for _, url := range urls {
		req, _ := http.NewRequest("GET", url+"/services", nil)
		req.Header.Set("content-type", "application/json")
		resp, err := DefaultClient.Do(req)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to connect to: %s/services\n", url)
		} else {
			body, err := ioutil.ReadAll(resp.Body)
			if err == nil {
				additionalServices := []string{}
				json.Unmarshal(body, &additionalServices)
				services = append(services, additionalServices...)
			}
			resp.Body.Close()
		}
	}
	servicelist := removeDuplicates(services)
	return servicelist
}

func removeDuplicates(a []string) []string {
	result := []string{}
	seen := map[string]byte{}
	for _, val := range a {
		if _, ok := seen[val]; !ok {
			result = append(result, val)
			seen[val] = 0
		}
	}
	return result
}

func includes(arr []string, elem string) bool {
	for _, element := range arr {
		if element == elem {
			return true
		}
	}
	return false
}

func getUrls(servers string) []string {
	serverlist := []string{}
	// Split to endpoints
	for _, host := range strings.Split(servers, ",") {
		// Split to host port pairs
		pair := strings.Split(host, ":")
		if len(pair) == 1 {
			pair = append(pair, "8080") //default port incase was not specified
		}
		addrs, err := net.LookupHost(pair[0])
		if err == nil {
			for _, addr := range addrs {
				serverlist = append(serverlist, "http://"+addr+":"+pair[1])
			}
		}
	}
	return serverlist
}

func prettyMessagePrint(jsonmsg map[string]interface{}, msgOnly bool, isEvents bool) {
	if msgOnly && !isEvents {
		if _, ok := jsonmsg["level"]; !ok {
			jsonmsg["level"] = interface{}("NONE")
		}
		if _, ok := jsonmsg["message"]; !ok {
			jsonmsg["message"] = interface{}("Missing message field - msg-only requires the message field to work")
		}
		if _, ok := jsonmsg["host"]; !ok {
			if _, ok := jsonmsg["kubernetes"]; !ok {
				jsonmsg["host"] = interface{}("UNKNOWN")
			} else {
				jsonmsg["host"] = interface{}(jsonmsg["kubernetes"].(map[string]interface{})["pod_name"].(string))
			}
		}
		ts := jsonmsg["@timestamp"].(time.Time).String()
		if jsonmsg["@timestamp"].(time.Time).Unix() == 0 {
			ts = "missing-timestamp"
		}
		jsonmsg = map[string]interface{}{"@timestamp": ts, "level": jsonmsg["level"].(string), "pod_name": (jsonmsg["kubernetes"].(map[string]interface{}))["pod_name"].(string), "message": jsonmsg["message"].(string)}
	}
	bytesmsg, _ := prettyjson.Marshal(jsonmsg)
	fmt.Printf("%s\n-----------------------------------------------------\n", string(bytesmsg))
}

func messagePrint(jsonmsg map[string]interface{}, msgOnly bool, isEvents bool, tmpl *template.Template) {
	if msgOnly && !isEvents {
		if _, ok := jsonmsg["level"]; !ok {
			jsonmsg["level"] = "NONE"
		}
		if _, ok := jsonmsg["message"]; !ok {
			jsonmsg["message"] = "Missing message field - msg-only requires the message field to work"
		}
		if _, ok := jsonmsg["host"]; !ok {
			if _, ok := jsonmsg["kubernetes"]; !ok {
				jsonmsg["host"] = "UNKNOWN"
			} else {
				if _, ok := jsonmsg["kubernetes"].(map[string]interface{})["pod_name"]; !ok {
					jsonmsg["host"] = "UNKNOWN_POD_NAME"
				} else {
					jsonmsg["host"] = jsonmsg["kubernetes"].(map[string]interface{})["pod_name"].(string)
				}
			}
		}
		e, err := domain.FromJsonBlob(jsonmsg)
		if err != nil {
			showRawEvent(jsonmsg)
		} else {
			showEvent(*e, tmpl)
		}
	} else {
		msg, ok := json.Marshal(jsonmsg)
		if ok != nil {
			fmt.Printf("Failed to marshal json from message: %v\n", jsonmsg)
		} else {
			fmt.Printf("%s\n", msg)
		}
	}
}

func showRawEvent(jsonmsg map[string]interface{}) {
	fmt.Printf("%s\t%s\t%s:\t%s\n", jsonmsg["@timestamp"], jsonmsg["level"], jsonmsg["host"], jsonmsg["message"])
	if exception, ok := jsonmsg["exception"]; ok {
		fmt.Printf("%s\n", exception.(string))
	}
}

func showEvent(event domain.Event, tmpl *template.Template) {
	err := tmpl.Execute(os.Stdout, event)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error with template %s\n", err)
	}
	if event.Exception != "" {
		fmt.Printf("%s\n", event.Exception)
	}
}

func printErrorAndExit(code int, format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "ERROR: %s\n", fmt.Sprintf(format, values...))
	fmt.Fprintln(os.Stderr)
	os.Exit(code)
}

func printUsageErrorAndExit(format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "ERROR: %s\n", fmt.Sprintf(format, values...))
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Available command line options:")
	flag.PrintDefaults()
	os.Exit(64)
}
