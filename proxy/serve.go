package main

import (
	"fmt"
	"io/ioutil"
	"io"
	"log"
	"net/http"
	"regexp"
	"os"
)

func copyHeader(dst, src http.Header) {
	for k, vv := range src {
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}

func main() {

	var s http.ServeMux

	var pngPath = regexp.MustCompile("^/.*\\.png$")
	var gifPath = regexp.MustCompile("^/.*\\.gif$")
	var cssPath = regexp.MustCompile("^/.*\\.css$")
	var htmlPath = regexp.MustCompile("^/.*\\.html$")
	var jsPath = regexp.MustCompile("^/.*\\.js$")
	var jsonPath = regexp.MustCompile("^/.*\\.json$")
	var woffPath = regexp.MustCompile("^/.*\\.woff2$")
	var svgPath = regexp.MustCompile("^/.*\\.svg$")
	var sparqlPath = regexp.MustCompile("^/sparql")
	var appPath = regexp.MustCompile("^/graph")

	var sketchy = regexp.MustCompile("\\.\\.")

	listen := ":8080"
	sparqlEndpoint := "sparql:8089"
	scheme := "http"
	base := ""

	if len(os.Args) > 1 {
		listen = os.Args[1]
	}

	// SPARQL endpoint in host:port form
	if len(os.Args) > 2 {
		sparqlEndpoint = os.Args[2]
	}

	if len(os.Args) > 3 {
		scheme = os.Args[3]
	}

	if len(os.Args) > 4 {
		base = os.Args[4]
	}

	s.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		
		fmt.Println(r.URL.Path)

		if m := sparqlPath.FindStringSubmatch(r.URL.Path); m != nil {

			// The bit of the path after /sparql
			path := r.URL.Path[7:]

			r.URL.Path = path
			r.URL.Host = sparqlEndpoint
			r.URL.Scheme = scheme
			r.Host = sparqlEndpoint

			resp, err := http.DefaultTransport.RoundTrip(r)

			if err != nil {
				log.Printf("503: %s", err.Error())
				http.Error(w, err.Error(),
					http.StatusServiceUnavailable)
				return
			}

			defer resp.Body.Close()
			copyHeader(w.Header(), resp.Header)
			w.Header().Set("Content-Type", "text/javascript")
			w.WriteHeader(resp.StatusCode)
			io.Copy(w, resp.Body)
			return

		}

		// Root URL just returns index.html
		if r.URL.Path == "/" {
			w.Header().Set("Content-Type", "text/html")
			filename := base + "/index.html"
			data, _ := ioutil.ReadFile(filename)
			w.Write(data)
			return
		}

		// /graph URLs return index.html also.  This is an Angular
		// app, Angular will deal with the URLs.
		if m := appPath.FindStringSubmatch(r.URL.Path); m != nil {
			w.Header().Set("Content-Type", "text/html")
			filename := base + "/index.html"
			data, _ := ioutil.ReadFile(filename)
			w.Write(data)
			return
		}

		// Catch all that path navigation junk that spews to the logs,
		// while also preventing navigation out of the web
		// directory.
		if m := sketchy.FindStringSubmatch(r.URL.Path); m != nil {
			http.NotFound(w, r)
			return
		}

		if m := cssPath.FindStringSubmatch(r.URL.Path); m != nil {
			w.Header().Set("Content-Type", "text/css")
			filename := base + r.URL.Path
			data, _ := ioutil.ReadFile(filename)
			w.Write(data)
			return
		}

		if m := pngPath.FindStringSubmatch(r.URL.Path); m != nil {
			w.Header().Set("Content-Type", "image/png")
			filename := base + r.URL.Path
			data, _ := ioutil.ReadFile(filename)
			w.Write(data)
			return
		}

		if m := gifPath.FindStringSubmatch(r.URL.Path); m != nil {
			w.Header().Set("Content-Type", "image/gif")
			filename := base + r.URL.Path
			data, _ := ioutil.ReadFile(filename)
			w.Write(data)
			return
		}

		if m := htmlPath.FindStringSubmatch(r.URL.Path); m != nil {
			w.Header().Set("Content-Type", "text/html")
			filename := base + r.URL.Path
			data, _ := ioutil.ReadFile(filename)
			w.Write(data)
			return
		}

		if m := jsPath.FindStringSubmatch(r.URL.Path); m != nil {
			w.Header().Set("Content-Type", "text/javascript")
			filename := base + r.URL.Path
			data, _ := ioutil.ReadFile(filename)
			w.Write(data)
			return
		}

		if m := jsonPath.FindStringSubmatch(r.URL.Path); m != nil {
			w.Header().Set("Content-Type", "text/javascript")
			filename := base + r.URL.Path
			data, _ := ioutil.ReadFile(filename)
			w.Write(data)
			return
		}

		if m := woffPath.FindStringSubmatch(r.URL.Path); m != nil {
			w.Header().Set("Content-Type", "text/plain")
			filename := base + r.URL.Path
			data, _ := ioutil.ReadFile(filename)
			w.Write(data)
			return
		}

		if m := svgPath.FindStringSubmatch(r.URL.Path); m != nil {
			w.Header().Set("Content-Type", "text/xml+svg")
			filename := base + r.URL.Path
			data, _ := ioutil.ReadFile(filename)
			w.Write(data)
			return
		}

		http.NotFound(w, r)
		return

	})

	log.Fatal(http.ListenAndServe(listen, &s))

}

