package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"

	"golang.org/x/net/html"
)

type Crawler struct {
	visited      map[string]bool
	brokenLinks  int
	brokenReport []string
	mu           sync.Mutex
}

func NewCrawler() *Crawler {
	return &Crawler{
		visited:      make(map[string]bool),
		brokenReport: []string{},
	}
}

func (c *Crawler) Visit(link string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.visited[link] {
		return false
	}
	c.visited[link] = true
	return true
}

func (c *Crawler) ReportBrokenLink(link string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.brokenLinks++
	c.brokenReport = append(c.brokenReport, link)
}

func (c *Crawler) Crawl(domain string, link string, depth int) {
	if depth <= 0 {
		return
	}

	if !c.Visit(link) {
		return
	}
	fmt.Println("Visiting:", link)

	parsedURL, err := url.Parse(link)
	if err != nil {
		fmt.Println("Error parsing URL:", link, err)
		return
	}

	// Properly encode query parameters
	parsedURL.RawQuery = parsedURL.Query().Encode()

	encodedLink := parsedURL.String() // Fully encoded URL
	fmt.Println("Encoded URL:", encodedLink)

	resp, err := http.Get(encodedLink)

	if err != nil {
		fmt.Println("Error fetching link:", link, err)
		c.ReportBrokenLink(link)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Println("Non-OK HTTP status:", resp.StatusCode, "for link:", link)
		c.ReportBrokenLink(link)
		return
	}

	links := extractLinks(resp.Body, domain)
	for _, l := range links {
		c.Crawl(domain, l, depth-1)
	}
}

func extractLinks(body io.Reader, domain string) []string {
	links := []string{}
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

	if parsedLink.Scheme == "" {
		parsedLink.Scheme = "http"
	}

	if parsedLink.Host == "" {
		parsedLink.Host = domain
	}

	if parsedLink.Host != domain {
		return ""
	}

	return parsedLink.String()
}

func main() {
	defaultURL := "http://127.0.0.1:8000"
	defaultDepth := 5

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
	crawler.Crawl(domain, *startURL, *depth)

	fmt.Printf("Broken links encountered: %d\n", crawler.brokenLinks)
	if crawler.brokenLinks > 0 {
		fmt.Println("Broken links:")
		for _, link := range crawler.brokenReport {
			fmt.Println("-", link)
		}
	}
}
