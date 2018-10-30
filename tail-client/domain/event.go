package domain

import (
	"encoding/json"
	"sort"
	"strings"
	"time"
)

type Message string
type ContextMap map[string]string
type Labels map[string]string
type Exception string
type Level string
type Host string

var Location = time.UTC

// Event  see https://github.com/apache/logging-log4j2/blob/master/log4j-core/src/main/java/org/apache/logging/log4j/core/LogEvent.java
type Event struct {
	App            string
	Host           Host `json:"host"`
	Kubernetes     Kubernetes
	LoggerName     string `json:"loggerName"`
	Level          Level
	Thread         string
	ThreadPriority int
	Timestamp      Timestamp  `json:"@timestamp"`
	Instant        Instant    `json:"instant"`
	Docker         Docker     `json:"docker"`
	ContextMap     ContextMap `json:"contextMap"`
	ThreadId       int16
	Stream         string
	Index        string
	Tags           []string
	Type           string
	ObMethod       string
	Message        Message // so I can encode newlines
	Exception      Exception
}

type Instant struct {
	EpochSecond  int64
	NanoOfSecond int64
}

type Kubernetes struct {
	Container string `json:"container_name"`
	Host      Host
	PodId     string `json:"pod_id"`
	Namespace string `json:"namespace_name"`
	PodName   string `json:"pod_name"`
	Labels    Labels
}

type Docker struct {
	ContainerId string `json:"container_id"`
}

type Timestamp struct {
	Time time.Time
}

func (t *Timestamp) UnmarshalJSON(b []byte) error {
	if len(b) < 10 {
		t.Time = time.Time{}
		return nil
	}
	ts, err := time.Parse(time.RFC3339, string(b[1:len(b)-1]))
	if err != nil {
		t.Time = time.Time{}
	} else {
		t.Time = ts
	}
	return nil
}

func (t Timestamp) String() string {
	if t.Time.Unix() <= 0 {
		return "missing-timestamp"
	}
	return t.Time.In(Location).String()
}

func (l Level) String() string {
	if l == "" {
		return "NONE"
	}
	return string(l)
}

func (m Message) String() string {
	if m == "" {
		return "Missing message field - msg-only requires the message field to work"
	}
	return string(m)
}

func (m Exception) String() string {
	return string(m)
}

func (h Host) String() string {
	if h == "" {
		return "UNKNOWN"
	}
	return string(h)
}

func (c ContextMap) String() string {
	s := strings.Builder{}
	keys := sortKeys(c)
	for _, key := range keys {
		s.WriteString(key)
		s.WriteString("=")
		s.WriteString(c[key])
		s.WriteString(",")
	}
	context := s.String()
	if len(context) > 2 {
		return context[:len(context)-1]
	}
	return context
}

func (l Labels) String() string {
	s := strings.Builder{}
	if len(l) == 0 {
		return ""
	}
	for _, key := range sortKeys(l) {
		s.WriteString(key)
		s.WriteString("=")
		s.WriteString(l[key])
		s.WriteString(",")
	}
	labels := s.String()
	if len(labels) > 2 {
		return labels[:len(labels)-1]
	}
	return labels
}

func sortKeys(m map[string]string) []string {
	keys := make([]string, len(m))
	i := 0
	for k := range m {
		keys[i] = k
		i++
	}
	sort.Strings(keys)
	return keys
}

func (t Instant) String() string {
	if t.EpochSecond <= 0 {
		return "missing-timestamp"
	}
	return time.Unix(t.EpochSecond, t.NanoOfSecond).In(Location).String()
}

func FromJsonBlob(jsonMsg map[string]interface{}) (*Event, error) {
	b, err := json.Marshal(jsonMsg)
	if err != nil {
		return nil, err
	}
	var e Event
	err = json.Unmarshal(b, &e)
	e.Host = Host(jsonMsg["host"].(string))
	e.Kubernetes.PodName = e.Host.String()
	if e.Instant.EpochSecond != 0 {
		e.Timestamp.Time = time.Unix(e.Instant.EpochSecond, e.Instant.NanoOfSecond)
	} else {
		t := jsonMsg["@timestamp"].(time.Time)
		e.Instant = Instant{EpochSecond: t.Unix(), NanoOfSecond: int64(t.Nanosecond())}
	}
	return &e, nil
}
