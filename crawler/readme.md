# Web Crawler in Go

This Go program implements a simple web crawler that traverses links on a given domain up to a specified depth. It tracks visited links, identifies broken links, and provides a report of broken or inaccessible links at the end of the crawl.

**Note:** This program was built for a local instance of the `find-moj-data` app with disabled authentication.

## Features

- Crawls all links on a given domain up to a configurable depth.
- Tracks and avoids revisiting previously visited links.
- Detects broken links (e.g., HTTP errors, non-200 responses).
- Reports the total number of broken links and their URLs.

## Requirements

- Go 1.19 or later.

## Installation

1. Clone the repository or copy the source code into a file named `webcrawler.go`.
2. Install the required dependencies:
   ```bash
   go mod tidy
   ```

## Usage

Build and run the program with the following steps:

### Build

```bash
go build -o webcrawler
```

### Run

```bash
./webcrawler -url=<starting_url> -depth=<crawl_depth>
```

### Example

To start crawling from `http://127.0.0.1:8000` with a depth of 3:

```bash
./webcrawler -url=https://127.0.0.1:8000 -depth=3
```

### Command-Line Flags

- `-url`: The starting URL for the crawler. Defaults to `https://example.com`.
- `-depth`: The depth to crawl. Defaults to `2`.

## How It Works

1. **Initialization**:
   - The crawler starts from the provided URL and crawls all links belonging to the same domain.

2. **Link Extraction**:
   - Extracts and resolves valid links from `<a>` tags in the HTML document.
   - Filters out links that point to external domains.

3. **Error Handling**:
   - Handles HTTP errors, invalid URLs, and non-OK status codes by logging them as broken links.

4. **Reporting**:
   - After completing the crawl, the program displays the total number of broken links encountered and lists their URLs.

## Example Output

```plaintext
Visiting: https://example.com
Visiting: https://example.com/page1
Error fetching link: https://example.com/broken-page 404 Not Found
Visiting: https://example.com/page2

Broken links encountered: 1
Broken links:
- https://example.com/broken-page
```

## Notes

- The crawler strips spaces from URLs before making requests.
- It only crawls links within the same domain as the starting URL.

## License

This project is licensed under the MIT License.

