package elasticsearch

import (
	"context"
	"log"

	"github.com/sciffer/sse"
	"gopkg.in/olivere/elastic.v6"
)

type elasticsearch struct {
	client       elastic.Client
	indices      []string
	relativetime string
	filters      []elastic.Query
}

func NewElasticsearch(cluster string, indices []string, service string, relativetime string) *elasticsearch {
	if newClient, err := elastic.NewClient(elastic.SetURL(cluster)); err == nil {
		return &elasticsearch{client: *newClient, indices: indices, relativetime: relativetime, filters: []elastic.Query{elastic.NewTermQuery("app.keyword", service)}}
	}
	log.Fatalf("Failed to create client for:%s", cluster)
	return nil
}

func (e *elasticsearch) Query2sse(outputStream chan *sse.Event, terms map[string]interface{}, msgCount int) error {
	//Assemble query from terms list
	if len(terms) > 0 {
		for k, v := range terms {
			if _, ok := v.([]interface{}); ok {
				e.filters = append(e.filters, elastic.NewTermsQuery(k, v.([]interface{})...))
			} else if _, ok := v.([]string); ok {
				tmp := make([]interface{}, len(v.([]string)))
				for i, val := range v.([]string) {
					tmp[i] = val
				}
				e.filters = append(e.filters, elastic.NewTermsQuery(k, tmp...))
			} else {
				e.filters = append(e.filters, elastic.NewTermsQuery(k, v))
			}
		}
	}

	//Perform query
	ctx := context.Background()
	result, err := e.client.Search().
		Index(e.indices...).
		Query(elastic.NewBoolQuery().Must(elastic.NewRangeQuery("@timestamp").Gt(e.relativetime)).Filter(e.filters...)).
		Sort("@timestamp", true).
		Size(msgCount).
		Do(ctx)
	//Write reults to sse.Event channel
	if err == nil {
		if result.Hits.TotalHits > 0 {
			for _, event := range result.Hits.Hits {
				if data, err := event.Source.MarshalJSON(); err == nil {
					outputStream <- &sse.Event{Data: data}
				}
			}
		}
	}

	return err
}
