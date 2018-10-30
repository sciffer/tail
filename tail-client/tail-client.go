package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/sciffer/tail/tail-client/tailclient"
	ctemplate "github.com/sciffer/tail/tail-client/template"
)

var (
	version string

	service         = flag.String("service", "", "The service name you want to tail")
	clusters        = flag.String("cluster", "", "The cluster/s name to filter by, if set only messages with matching kubecluster pod labels will show up")
	pods            = flag.String("pod", "", "The pod name/s you want to tail, if more than 1 use comma as seperator")
	env             = flag.String("env", "", "The environment you want to tail, like: prod, stg, etc...")
	podid           = flag.String("podid", "", "The pod id you want to tail")
	rev             = flag.String("rev", "", "The revision/version to tail, filter based on the version field")
	servers         = flag.String("server", "tail1:8080,tail2:8080", "The comma delimited list of event endpoints(<server>:<port>) to connect to.")
	uri             = flag.String("uri", "/events", "The uri prefix used for events streaming")
	pretty          = flag.Bool("pretty", false, "Whether to turn on pretty print of json")
	msgOnly         = flag.Bool("msg-only", false, "Whether to print only the message with timestamp and podname")
	isEvents        = flag.Bool("events", false, "Whether see events instead of logs, defaults to false.")
	bufferSize      = flag.Int("buffer-size", 256, "The buffer size of the message channel.")
	timezone        = flag.String("timezone", "America/New_York", "Timezone to display logs in, IANA format, like: America/New_York , UTC, etc...")
	fieldsArg       = flag.String("fields", strings.Join(ctemplate.DefaultFields, ","), "list of fields to display separated by tabs \\t")
	elasticClusters = flag.String("esclusters", "escluster1:9200,escluster2:9200", "Comma delimited list of es6 cluster names§(for history only)")
	timeOffset      = flag.String("from", "now-15m", "Elasticsearch6 relative time to query from(for history only)")
	indices         = flag.String("indices", "logs-*", "Comma delimited list of index patterns(for history only)")
	eventsindices   = flag.String("eventsindices", "events-*", "Comma delimited list of events index patterns(for history only)")
	history         = flag.Bool("history", false, "query events from history/elasticsearch, instead of live events")
	maxMessages     = flag.Int("max-msg", 10000, "The maximum amount of messages to display(for history only)")
	showFields      = flag.Bool("show-fields", false, "show list of fields")
)

func includes(arr []string, elem string) bool {
	for _, element := range arr {
		if element == elem {
			return true
		}
	}
	return false
}

func main() {
	fmt.Printf("Tail-client %s\n", version)
	flag.Parse()
	if *showFields {
		fmt.Printf("\nValid field names:\n")
		for _, field := range ctemplate.GetValidFields() {
			fmt.Printf("%s\n", field)
		}
		os.Exit(0)
	}

	client := ctailclient.NewCtailClient(*servers, *uri, *service, *fieldsArg, *history, *timezone, *bufferSize)

	services := client.GetServices([]string{})
	if *service == "" {
		printUsageErrorAndExit("-service is required, please specify one of the following services: " + strings.Join(services, " ,"))
	} else {
		if !includes(services, *service) {
			printUsageErrorAndExit("-service not found, please specify one of the following services: " + strings.Join(services, " ,"))
		}
	}

	client.SetFilters(*pods, *clusters, *podid, *env, *rev)

	fmt.Println("Starting client Subscribe")

	if *history {
		if *isEvents {
			client.SetHistoryParams(*elasticClusters, *eventsindices, *maxMessages, *timeOffset)
		} else {
			client.SetHistoryParams(*elasticClusters, *indices, *maxMessages, *timeOffset)
		}
		client.Subscribe2Elasticsearch()
	} else {
		client.Subscribe2CtailServers()
	}

	// Consume and print logs/events
	client.ConsumeAndPrint(*isEvents, *pretty, *msgOnly)
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
