package main

import (
	"flag"
	"fmt"
	"golang.org/x/net/html"
	"io"
	"net/http"
	"net/url"
	"strings"
)

type Crawler struct {
	visited          map[string]bool
	brokenLinks      int
	brokenReport     []string
	brokenSources    map[string]string
	secondaryDomains map[string]bool
}

func NewCrawler() *Crawler {
	return &Crawler{
		visited:       make(map[string]bool),
		brokenReport:  []string{},
		brokenSources: map[string]string{},
		secondaryDomains: map[string]bool{
			"data.justice.gov.uk": true,
			"www.gov.uk":          true,
			"criminal-justice-delivery-data-dashboards.justice.gov.uk": true,
		},
	}
}

func (c *Crawler) Visit(link string) bool {
	if c.visited[link] {
		return false
	}
	c.visited[link] = true
	return true
}

func (c *Crawler) ReportBrokenLink(link string, source string) {
	c.brokenLinks++
	c.brokenReport = append(c.brokenReport, link)
	c.brokenSources[link] = source

}

func (c *Crawler) Crawl(domain string, link string, source string, depth int) {
	if depth <= 0 {
		return
	}

	if strings.HasPrefix(link, "http://127.0.0.1:8000/feedback/") {
		return
	}

	if !c.Visit(link) {
		return
	}

	parsedURL, err := url.Parse(link)
	if err != nil {
		fmt.Println("Error parsing URL:", link, err)
		return
	}

	// Skip links outside the main domain and secondary domains
	if parsedURL.Host != domain && !c.secondaryDomains[parsedURL.Host] {
		return
	}

	fmt.Println("Visiting:", link)

	// Properly encode query parameters
	parsedURL.RawQuery = parsedURL.Query().Encode()

	resp, err := http.Get(link)
	if err != nil {
		fmt.Println("Error fetching link:", link, err)
		c.ReportBrokenLink(link, source)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Println("Non-OK HTTP status:", resp.StatusCode, "for link:", link)
		c.ReportBrokenLink(link, source)
		return
	}

	// Stop crawling further for secondary domains
	if c.secondaryDomains[parsedURL.Host] {
		fmt.Println("Checked link within secondary domain:", link)
		return
	}

	// Extract and crawl links within the main domain
	links := extractLinks(resp.Body, domain)
	for _, l := range links {
		c.Crawl(domain, l, link, depth-1)
	}
}

func extractLinks(body io.Reader, domain string) []string {
	var links []string
	page := html.NewTokenizer(body)

	for {
		tokenType := page.Next()
		if tokenType == html.ErrorToken {
			break
		}

		token := page.Token()
		if tokenType == html.StartTagToken && token.Data == "a" {
			for _, attr := range token.Attr {
				if attr.Key == "href" {
					link := resolveLink(attr.Val, domain)
					if link != "" {
						links = append(links, link)
					}
				}
			}
		}
	}

	return links
}

func resolveLink(href, domain string) string {
	parsedLink, err := url.Parse(href)
	if err != nil {
		return ""
	}

	// Ignore non-HTTP(S) schemes like mailto:
	if parsedLink.Scheme != "" && parsedLink.Scheme != "http" && parsedLink.Scheme != "https" {
		return ""
	}

	if parsedLink.Scheme == "" {
		parsedLink.Scheme = "http"
	}

	if parsedLink.Host == "" {
		parsedLink.Host = domain
	}

	// Normalize URL: Remove fragments and trailing slashes
	parsedLink.Fragment = ""
	parsedLink.Path = normalizePath(parsedLink.Path)

	return parsedLink.String()
}

func normalizePath(path string) string {
	if len(path) > 1 && path[len(path)-1] == '/' {
		return path[:len(path)-1]
	}
	return path
}

func main() {
	defaultURL := "http://127.0.0.1:8000"
	defaultDepth := 7

	startURL := flag.String("url", defaultURL, "Starting URL for the crawler")
	depth := flag.Int("depth", defaultDepth, "Depth to crawl")
	flag.Parse()

	parsedURL, err := url.Parse(*startURL)
	if err != nil {
		fmt.Println("Invalid URL:", err)
		return
	}

	domain := parsedURL.Host
	crawler := NewCrawler()
	crawler.Crawl(domain, *startURL, *startURL, *depth)

	fmt.Printf("Broken links encountered: %d\n", crawler.brokenLinks)
	if crawler.brokenLinks > 0 {
		fmt.Println("Broken links and their sources:")
		for link, source := range crawler.brokenSources {
			fmt.Printf("- %s (found on: %s)\n", link, source)
		}
	}
}
