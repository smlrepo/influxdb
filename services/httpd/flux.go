package httpd

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"mime"
	"net/http"
	"unicode/utf8"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/csv"
	"github.com/influxdata/flux/lang"
	"github.com/influxdata/platform/query"
	"github.com/pkg/errors"
)

// QueryRequest is a flux query request.
type QueryRequest struct {
	Spec    *flux.Spec   `json:"spec,omitempty"`
	Query   string       `json:"query"`
	Type    string       `json:"type"`
	Dialect QueryDialect `json:"dialect"`
}

// QueryDialect is the formatting options for the query response.
type QueryDialect struct {
	Header         *bool    `json:"header"`
	Delimiter      string   `json:"delimiter"`
	CommentPrefix  string   `json:"commentPrefix"`
	DateTimeFormat string   `json:"dateTimeFormat"`
	Annotations    []string `json:"annotations"`
}

// WithDefaults adds default values to the request.
func (r QueryRequest) WithDefaults() QueryRequest {
	if r.Type == "" {
		r.Type = "flux"
	}
	if r.Dialect.Delimiter == "" {
		r.Dialect.Delimiter = ","
	}
	if r.Dialect.DateTimeFormat == "" {
		r.Dialect.DateTimeFormat = "RFC3339"
	}
	if r.Dialect.Header == nil {
		header := true
		r.Dialect.Header = &header
	}
	return r
}

// Validate checks the query request and returns an error if the request is invalid.
func (r QueryRequest) Validate() error {
	if r.Query == "" && r.Spec == nil {
		return errors.New(`request body requires either spec or query`)
	}

	if r.Type != "flux" {
		return fmt.Errorf(`unknown query type: %s`, r.Type)
	}

	if len(r.Dialect.CommentPrefix) > 1 {
		return fmt.Errorf("invalid dialect comment prefix: must be length 0 or 1")
	}

	if len(r.Dialect.Delimiter) != 1 {
		return fmt.Errorf("invalid dialect delimeter: must be length 1")
	}

	rn, size := utf8.DecodeRuneInString(r.Dialect.Delimiter)
	if rn == utf8.RuneError && size == 1 {
		return fmt.Errorf("invalid dialect delimeter character")
	}

	for _, a := range r.Dialect.Annotations {
		switch a {
		case "group", "datatype", "default":
		default:
			return fmt.Errorf(`unknown dialect annotation type: %s`, a)
		}
	}

	switch r.Dialect.DateTimeFormat {
	case "RFC3339", "RFC3339Nano":
	default:
		return fmt.Errorf(`unknown dialect date time format: %s`, r.Dialect.DateTimeFormat)
	}

	return nil
}

// ProxyRequest specifies a query request and the dialect for the results.
type ProxyRequest struct {
	// Request is the basic query request
	Request query.Request `json:"request"`

	// Dialect is the result encoder
	Dialect flux.Dialect `json:"dialect"`
}

// ProxyRequest returns a request to proxy from the flux.
func (r QueryRequest) ProxyRequest() *ProxyRequest {
	// Query is preferred over spec
	var compiler flux.Compiler
	if r.Query != "" {
		compiler = lang.FluxCompiler{
			Query: r.Query,
		}
	} else if r.Spec != nil {
		compiler = lang.SpecCompiler{
			Spec: r.Spec,
		}
	}

	delimiter, _ := utf8.DecodeRuneInString(r.Dialect.Delimiter)

	noHeader := false
	if r.Dialect.Header != nil {
		noHeader = !*r.Dialect.Header
	}

	cfg := csv.DefaultEncoderConfig()
	cfg.NoHeader = noHeader
	cfg.Delimiter = delimiter

	// TODO(nathanielc): Use commentPrefix and dateTimeFormat
	// once they are supported.
	return &ProxyRequest{
		Request: query.Request{
			Compiler: compiler,
		},
		Dialect: csv.Dialect{
			ResultEncoderConfig: cfg,
		},
	}
}

// httpDialect is an encoding dialect that can write metadata to HTTP headers
type httpDialect interface {
	SetHeaders(w http.ResponseWriter)
}

func decodeQueryRequest(r *http.Request) (*QueryRequest, error) {
	ct := r.Header.Get("Content-Type")
	mt, _, err := mime.ParseMediaType(ct)
	if err != nil {
		return nil, err
	}

	var req QueryRequest
	switch mt {
	case "application/vnd.flux":
		if d, err := ioutil.ReadAll(r.Body); err != nil {
			return nil, err
		} else {
			req.Query = string(d)
		}
	default:
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			return nil, err
		}
	}

	req = req.WithDefaults()
	err = req.Validate()
	if err != nil {
		return nil, err
	}

	return &req, err
}
