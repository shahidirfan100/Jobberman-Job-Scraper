# Jobberman.com Jobs Scraper

This Apify actor scrapes job listings from Jobberman.com.

## Features

- Scrapes Jobberman.com job search results.
- Extracts detailed job information including title, company, location, and full description.
- Handles pagination to collect multiple pages of results.
- Can be configured to use proxies for reliable scraping.
- Saves results to a dataset.

## Input

The actor accepts the following input fields:

- `keyword`: The job title or keywords to search for.
- `location`: The geographic location to filter jobs by.
- `posted_date`: Filter jobs by when they were posted (e.g., "24h", "7d", "30d").
- `startUrl`: A specific Jobberman.com search URL to start scraping from.
- `results_wanted`: The maximum number of jobs to scrape.
- `max_pages`: A safety cap on the number of listing pages to visit.
- `collectDetails`: If enabled, the actor will visit each job's detail page to extract the full description and other details.
- `cookies`: Custom cookies to use for the requests, which can help bypass banners.
- `proxyConfiguration`: Proxy settings for the scraper.

## Output

The actor outputs a dataset of job listings with the following fields:

- `url`: The URL of the job posting.
- `title`: The job title.
- `company`: The company name.
- `location`: The job location.
- `date_posted`: When the job was posted.
- `description_html`: The job description in HTML format.
- `description_text`: The job description in plain text.