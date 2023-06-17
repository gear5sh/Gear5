package utils

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
)

type Request struct {
	URN         string
	Method      string
	Body        io.Reader
	QueryParams map[string]any
	Headers     map[string]any
}

func (r *Request) ToHTTPRequest() (*http.Request, error) {
	// Create a URL object from the URN
	urlObj, err := url.Parse(r.URN)
	if err != nil {
		return nil, err
	}

	// Create a new HTTP request with the specified method, body, and URL
	req, err := http.NewRequest(r.Method, urlObj.String(), r.Body)
	if err != nil {
		return nil, err
	}

	// Set query parameters
	query := urlObj.Query()
	for key, value := range r.QueryParams {
		query.Add(key, fmt.Sprintf("%v", value))
	}
	urlObj.RawQuery = query.Encode()

	// Set headers
	for key, value := range r.Headers {
		req.Header.Set(key, fmt.Sprintf("%v", value))
	}

	return req, nil
}
