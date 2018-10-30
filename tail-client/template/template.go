package template

import (
	"fmt"
	"sort"
	"strings"
	"text/template"
)

var DefaultFields = []string{
	"timestamp",
	"level",
	"host",
	"message",
}

var validFields = map[string]string{
	"timestamp":     ".Timestamp",
	"level":         ".Level",
	"host":          ".Host",
	"message":       ".Message",
	"app":           ".App",
	"loggerName":    ".LoggerName",
	"thread":        ".Thread",
	"threadId":      ".ThreadId",
	"owner":         "index .Kubernetes.Labels \"owner\"",
	"obMethod":      ".ObMethod",
	"exception":     ".Exception",
	"containerId":   ".Docker.ContainerId",
	"containerHost": ".Kubernetes.Host",
	"containerName": ".Kubernetes.Container",
	"podId":         ".Kubernetes.PodId",
	"pod":           ".Kubernetes.PodName",
	"env":           "index .Kubernetes.Labels \"environment\"",
	"version":       "index .Kubernetes.Labels \"version\"",
	"cluster":       "index .Kubernetes.Labels \"kubeCluster\"",
}

func CreateTemplate(fields []string) (*template.Template, error) {
	if len(fields) == 0 {
		return nil, fmt.Errorf("No fields provided")
	}
	templateBuilder := strings.Builder{}
	for _, field := range fields {
		if _, ok := validFields[field]; !ok {
			return nil, fmt.Errorf("field (%s) is not a valid field", field)
		}
		templateBuilder.WriteString(fmt.Sprintf("{{%s}}\t", validFields[field]))
	}
	var tmp = templateBuilder.String()
	tmp = tmp[0 : len(tmp)-1] // remove last \t

	return template.New("ctail-client").Parse(tmp + "\n")
}

func GetValidFields() []string {
	fieldNames := make([]string, len(validFields))
	i := 0
	for key := range validFields {
		fieldNames[i] = key
		i++
	}
	sort.Strings(fieldNames)
	return fieldNames
}
