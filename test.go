package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
)

func main() {
	var resp *http.Response

	// create index
	resp = request("DELETE", "http://127.0.0.1:9200/concurrency-test", "", false)
	resp = request("PUT", "http://127.0.0.1:9200/concurrency-test", "", true)

	// add a doc with a single integer field and print it
	resp = request("PUT", "http://127.0.0.1:9200/concurrency-test/_doc/1", `{"int_field": 0}`, true)
	resp = request("GET", "http://127.0.0.1:9200/concurrency-test/_doc/1", "", true)
	fmt.Println(readBody(resp))

	// concurrent updates
	goroutines := 5
	updatesPerRoutine := 10
	maxRetries := 10

	wg := sync.WaitGroup{}
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(goroutine int) {
			for i := 0; i < updatesPerRoutine; i++ {
				request(
					"POST",
					fmt.Sprintf("http://127.0.0.1:9200/concurrency-test/_update/1?retry_on_conflict=%d", maxRetries),
					`{"script": "ctx._source.int_field += 1"}`,
					true,
				)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()

	// print final doc
	resp = request("GET", "http://127.0.0.1:9200/concurrency-test/_doc/1", "", true)
	fmt.Println(readBody(resp))
}

func request(method string, url string, body string, failOnBadResponse bool) *http.Response {
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	panicOnError(err)
	req.Header = http.Header{"Content-Type": []string{"application/json"}}

	client := http.Client{}
	resp, err := client.Do(req)
	panicOnError(err)

	if failOnBadResponse {
		panicOnBadResponse(resp)
	}

	return resp
}

func readBody(resp *http.Response) string {
	data, err := ioutil.ReadAll(resp.Body)
	panicOnError(err)
	return string(data)
}

func badResponse(resp *http.Response) bool {
	return resp.StatusCode/100 != 2
}

func panicOnBadResponse(resp *http.Response) {
	if badResponse(resp) {
		panicOnError(fmt.Errorf("%s %s failed with status code %d. Body: %s", resp.Request.Method, resp.Request.URL, resp.StatusCode, readBody(resp)))
	}
}

func panicOnError(err error) {
	if err != nil {
		panic(err)
	}
}
