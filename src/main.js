// Jobberman.com jobs scraper (CheerioCrawler)
// Runtime: Node.js 18, ESM ("type": "module")
// Uses apify@^3 and crawlee@^3

import { Actor, log } from 'apify';
import { CheerioCrawler, Dataset } from 'crawlee';
import { load as cheerioLoad } from 'cheerio';

// Health check variables
let healthCheckPassed = false;
let startTime = Date.now();
let lastActivityTime = Date.now();

await Actor.init();

// ------------------------- INPUT VALIDATION -------------------------
const input = await Actor.getInput() ?? {};
log.info('Received input:', input);

// Validate input
if (!input || typeof input !== 'object') {
    throw new Error('Invalid input: Input must be an object');
}

const {
    keyword = '',
    location = '',
    posted_date = 'anytime',
    results_wanted: RESULTS_WANTED_RAW = 100,
    max_pages: MAX_PAGES_RAW = 999,
    collectDetails = true,
    startUrl,
    url,
    startUrls,
    cookies,
    cookiesJson,
    proxyConfiguration,
} = input;

const hasSearchTerms = keyword || location;
const hasUrls = startUrl || url || (startUrls && startUrls.length > 0);

if (!hasSearchTerms && !hasUrls) {
    log.warning('No keyword, location, or URLs provided. Defaulting to browse results.');
}

const RESULTS_WANTED = Number.isFinite(+RESULTS_WANTED_RAW) ? Math.max(1, +RESULTS_WANTED_RAW) : Number.MAX_SAFE_INTEGER;
const MAX_PAGES = Number.isFinite(+MAX_PAGES_RAW) ? Math.max(1, +MAX_PAGES_RAW) : 999;

const validPostedDates = ['anytime', '24h', '7d', '30d'];
if (!validPostedDates.includes(posted_date)) {
    throw new Error(`Invalid posted_date: ${posted_date}. Valid values are: ${validPostedDates.join(', ')}`);
}

// Health check: Monitor activity
setInterval(() => {
    if (Date.now() - lastActivityTime > 290000) { // 4m 50s
        log.error('Actor has not shown activity for a long time. Exiting to prevent stall.');
        process.exit(1);
    }
}, 60000); // Check every minute

// ------------------------- HELPERS -------------------------

const safeJsonParse = (str) => {
    try { return JSON.parse(str); } catch (err) { log.debug(`Bad JSON-LD: ${err.message}`); return null; }
};

const USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0',
];

const buildStartUrl = (kw, loc, date) => {
    const url = new URL('https://www.jobberman.com/jobs');
    if (kw && String(kw).trim()) url.searchParams.set('q', String(kw).trim());
    if (loc && String(loc).trim()) url.searchParams.set('l', String(loc).trim());
    
    const dateMap = { '24h': '1 day', '7d': '7 days', '30d': '30 days' };
    if (date && dateMap[date]) {
        url.searchParams.set('created_at', dateMap[date]);
    }
    
    return url.href;
};

const toAbs = (href, base = 'https://www.jobberman.com') => {
    try {
        const abs = new URL(href, base).href;
        return /^https?:/i.test(abs) ? abs : null;
    } catch { return null; }
};

const collectJobLinks = ($) => {
    const links = new Set();
    
    // Primary selector: links containing '/listings/'
    $('a[href*="/listings/"]').each((_, a) => {
        const href = $(a).attr('href');
        if (href && href.includes('/listings/')) {
            const abs = toAbs(href);
            if (abs && !abs.includes('#') && !abs.includes('utm_')) {
                // Clean URL - remove query parameters
                const cleanUrl = abs.split('?')[0];
                links.add(cleanUrl);
            }
        }
    });
    
    // Fallback: look for job card containers
    if (links.size === 0) {
        $('.job-list li a, .job-card a, .search-result-item a, .job-item a').each((_, a) => {
            const href = $(a).attr('href');
            if (href) {
                const abs = toAbs(href);
                if (abs) {
                    const cleanUrl = abs.split('?')[0];
                    links.add(cleanUrl);
                }
            }
        });
    }
    
    return [...links];
};

const findNextUrl = ($, currentUrl) => {
    // Primary: Look for "next page" link
    const nextLink = $('a[href*="?page="]:contains("next")').attr('href') || 
                     $('a[rel="next"]').attr('href') || 
                     $('a:contains("Go to next page")').attr('href');
    
    if (nextLink) return toAbs(nextLink);

    // Fallback: Extract current page number and build next URL
    const currentPageMatch = currentUrl.match(/[?&]page=(\d+)/);
    if (currentPageMatch) {
        const currentPage = parseInt(currentPageMatch[1]);
        const nextPageUrl = currentUrl.replace(/([?&])page=\d+/, `$1page=${currentPage + 1}`);
        return nextPageUrl;
    } else if (currentUrl.includes('?')) {
        // Add page parameter
        return `${currentUrl}&page=2`;
    } else {
        // First pagination
        return `${currentUrl}?page=2`;
    }
};

const findBestDescriptionContainer = ($) => {
    const selectors = [
        '.job-description', // Primary container on Jobberman
        '.job-details__main',
        '[class*="job-description"]',
        '#job-description',
        'article.job-details',
        '.job-summary',
        '.job-content'
    ];
    for (const sel of selectors) {
        const el = $(sel).first();
        if (el.length) return el;
    }
    return $('body'); // Fallback
};

const cleanText = (text) => {
    return String(text || '')
        .replace(/[\r\n\t]+/g, ' ')
        .replace(/[^\u0020-\u007E\u00A0-\u024F\u1E00-\u1EFF]/g, '')
        .trim();
};

// Helper to navigate DOM using XPath-like path (converted to jQuery traversal)
const getElementByPath = ($, path) => {
    // XPath to CSS selector mapping for the specific structure
    const xpathMap = {
        // Title: /html/body/main/section/div[2]/div[2]/div[2]/div[1]/div[1]/div/article/div[2]/div[2]/h1
        'title': 'main section > div:nth-child(2) > div:nth-child(2) > div:nth-child(2) > div:nth-child(1) > div:nth-child(1) > div > article > div:nth-child(2) > div:nth-child(2) > h1',
        // Company: /html/body/main/section/div[2]/div[2]/div[2]/div[1]/div[1]/div/article/div[2]/div[2]/h2[1]
        'company': 'main section > div:nth-child(2) > div:nth-child(2) > div:nth-child(2) > div:nth-child(1) > div:nth-child(1) > div > article > div:nth-child(2) > div:nth-child(2) > h2:nth-child(1)',
        // Job Type: /html/body/main/section/div[2]/div[2]/div[2]/div[1]/div[1]/div/article/div[2]/div[2]/div[1]/a[2]
        'job_type': 'main section > div:nth-child(2) > div:nth-child(2) > div:nth-child(2) > div:nth-child(1) > div:nth-child(1) > div > article > div:nth-child(2) > div:nth-child(2) > div:nth-child(1) > a:nth-child(2)',
        // Location: /html/body/main/section/div[2]/div[2]/div[2]/div[1]/div[1]/div/article/div[2]/div[2]/div[1]/a[1]
        'location': 'main section > div:nth-child(2) > div:nth-child(2) > div:nth-child(2) > div:nth-child(1) > div:nth-child(1) > div > article > div:nth-child(2) > div:nth-child(2) > div:nth-child(1) > a:nth-child(1)',
        // Salary: /html/body/main/section/div[2]/div[2]/div[2]/div[1]/div[1]/div/article/div[2]/div[2]/div[2]/span
        'salary': 'main section > div:nth-child(2) > div:nth-child(2) > div:nth-child(2) > div:nth-child(1) > div:nth-child(1) > div > article > div:nth-child(2) > div:nth-child(2) > div:nth-child(2) > span',
        // Category: /html/body/main/section/div[2]/div[2]/div[2]/div[1]/div[1]/div/article/div[2]/div[2]/div[2]/a
        'category': 'main section > div:nth-child(2) > div:nth-child(2) > div:nth-child(2) > div:nth-child(1) > div:nth-child(1) > div > article > div:nth-child(2) > div:nth-child(2) > div:nth-child(2) > a',
        // Description: /html/body/main/section/div[2]/div[2]/div[2]/div[1]/div[1]/div/article/div[4]
        'description': 'main section > div:nth-child(2) > div:nth-child(2) > div:nth-child(2) > div:nth-child(1) > div:nth-child(1) > div > article > div:nth-child(4)',
    };
    
    return xpathMap[path] ? $(xpathMap[path]) : $();
};


const sanitizeDescription = ($, el, baseUrl) => {
    if (!el || !el.length) return '';
    const clone = el.clone();
    
    // Remove unwanted elements completely
    clone.find('script, style, noscript, svg, button, form, header, footer, nav, aside, iframe').remove();
    clone.find('[class*="social"], [class*="share"], [class*="apply"], [class*="login"]').remove();
    clone.find('[class*="banner"], [class*="ad-"], [class*="advertisement"]').remove();
    clone.find('[id*="ad-"], [id*="banner"], [class*="cookie"]').remove();
    
    // Allowed text-related HTML tags (keep structure, remove everything else)
    const allowedTags = ['p', 'br', 'strong', 'b', 'em', 'i', 'u', 'ul', 'ol', 'li', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'a', 'span', 'div', 'section', 'article'];
    
    // Remove all attributes from all elements (including class, id, style)
    clone.find('*').each((_, node) => {
        const $node = $(node);
        const tagName = node.tagName ? node.tagName.toLowerCase() : '';
        
        // Remove all attributes first
        const attribs = Object.keys(node.attribs || {});
        attribs.forEach(attr => $node.removeAttr(attr));
        
        // For anchor tags, keep only href (cleaned)
        if (tagName === 'a') {
            const originalHref = $(node).attr('href');
            if (originalHref) {
                const abs = toAbs(originalHref, baseUrl);
                if (abs) $node.attr('href', abs);
            }
        }
        
        // Remove non-allowed tags but keep their text content
        if (!allowedTags.includes(tagName)) {
            const text = $node.text();
            if (text.trim()) {
                $node.replaceWith(text);
            } else {
                $node.remove();
            }
        }
    });

    // Remove empty elements (multiple passes for nested empties)
    for (let i = 0; i < 5; i++) {
        const empties = clone.find('*').filter((_, n) => {
            const $n = $(n);
            const text = $n.text().trim();
            const hasChildren = $n.children().length > 0;
            const isBreakOrImg = n.tagName === 'br' || n.tagName === 'img' || n.tagName === 'hr';
            return !text && !hasChildren && !isBreakOrImg;
        });
        if (empties.length === 0) break;
        empties.remove();
    }
    
    // Unwrap unnecessary nested divs/spans that have no attributes
    for (let i = 0; i < 3; i++) {
        clone.find('div, span, section, article').each((_, el) => {
            const $el = $(el);
            // If it has no attributes and only one child, unwrap it
            if (Object.keys(el.attribs || {}).length === 0) {
                const children = $el.children();
                if (children.length === 1) {
                    $el.replaceWith(children);
                }
            }
        });
    }

    let html = clone.html() || '';
    clone.remove();
    
    // Clean up excessive whitespace but preserve structure
    html = html
        .replace(/\s+/g, ' ')           // Multiple spaces to one
        .replace(/>\s+</g, '><')        // Remove space between tags
        .replace(/<br\s*\/?>\s*/gi, '<br>') // Clean br tags
        .trim();
    
    return html;
};

const normalizeCookieHeader = ({ cookies, cookiesJson }) => {
    if (cookies && typeof cookies === 'string' && cookies.trim()) return cookies.trim();
    if (cookiesJson && typeof cookiesJson === 'string') {
        try {
            const parsed = JSON.parse(cookiesJson);
            if (Array.isArray(parsed)) {
                return parsed.map(c => `${c.name}=${c.value}`).join('; ');
            }
            if (typeof parsed === 'object') {
                return Object.entries(parsed).map(([k, v]) => `${k}=${v}`).join('; ');
            }
        } catch (e) {
            log.debug('Could not parse cookiesJson', { error: e.message });
        }
    }
    return '';
};

// ------------------------- START URLS -------------------------
const builtStartUrl = buildStartUrl(keyword, location, posted_date);
const initialUrls = [];
if (Array.isArray(startUrls) && startUrls.length) initialUrls.push(...startUrls);
if (startUrl) initialUrls.push(startUrl);
if (url) initialUrls.push(url);
if (initialUrls.length === 0) initialUrls.push(builtStartUrl);

// ------------------------- PROXY & CRAWLER SETUP -------------------------
const proxyConf = proxyConfiguration ? await Actor.createProxyConfiguration(proxyConfiguration) : undefined;

let jobsScraped = 0;
let jobsEnqueued = 0;
const scrapedUrls = new Set();

const crawler = new CheerioCrawler({
    proxyConfiguration: proxyConf,
    maxRequestsPerMinute: 120, // Slower to be gentler
    requestHandlerTimeoutSecs: 45,
    navigationTimeoutSecs: 45,
    maxConcurrency: 5,
    useSessionPool: true,
    persistCookiesPerSession: true,
    sessionPoolOptions: {
        maxPoolSize: 50,
        sessionOptions: {
            maxUsageCount: 50,
            maxErrorScore: 3,
        },
    },
    maxRequestRetries: 5,
    maxRequestsPerCrawl: Math.max(RESULTS_WANTED * 3, 1000),
    
    preNavigationHooks: [({ request }) => {
        request.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': request.userData?.label === 'DETAIL' ? 'same-origin' : 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'no-cache',
            'User-Agent': USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)],
            'Referer': request.userData?.label === 'DETAIL' ? 'https://www.jobberman.com/jobs' : undefined,
        };
        const cookieHeader = normalizeCookieHeader({ cookies, cookiesJson });
        if (cookieHeader) request.headers.Cookie = cookieHeader;
    }],

    async requestHandler({ request, $, log: crawlerLog, enqueueLinks, session }) {
        lastActivityTime = Date.now();
        const { label = 'LIST', pageNo = 1 } = request.userData;

        if (session) session.markGood();

        if (label === 'LIST') {
            const links = collectJobLinks($);
            crawlerLog.info(`LIST page ${pageNo}: Found ${links.length} potential jobs.`);

            if (links.length === 0 && pageNo > 1) {
                crawlerLog.warning(`Page ${pageNo} has no jobs. This may be the end of results.`);
            }

            if (!collectDetails) {
                const toPush = links.slice(0, RESULTS_WANTED - jobsScraped);
                if (toPush.length > 0) {
                    await Dataset.pushData(toPush.map(url => ({ url, _source: 'jobberman.com' })));
                    jobsScraped += toPush.length;
                }
            } else {
                const toEnqueue = links.slice(0, RESULTS_WANTED - jobsEnqueued);
                if (toEnqueue.length > 0) {
                    await enqueueLinks({ urls: toEnqueue, userData: { label: 'DETAIL' } });
                    jobsEnqueued += toEnqueue.length;
                }
            }

            if (jobsScraped >= RESULTS_WANTED || jobsEnqueued >= RESULTS_WANTED || pageNo >= MAX_PAGES) {
                crawlerLog.info('Stopping pagination based on limits.');
                return;
            }

            const nextUrl = findNextUrl($, request.url);
            if (nextUrl) {
                crawlerLog.info(`Enqueuing next list page: ${pageNo + 1}`);
                await enqueueLinks({
                    urls: [nextUrl],
                    userData: { label: 'LIST', pageNo: pageNo + 1 },
                    forefront: true,
                });
            } else {
                crawlerLog.info(`No next page found on page ${pageNo}.`);
            }
        }

        if (label === 'DETAIL') {
            if (jobsScraped >= RESULTS_WANTED || scrapedUrls.has(request.url)) {
                return;
            }

            let title, company, location, date_posted, job_type, salary_range, category, description_html, description_text;

            try {
                // Try JSON-LD first
                const jsonLdScript = $('script[type="application/ld+json"]').html();
                const jsonLd = jsonLdScript ? safeJsonParse(jsonLdScript) : null;

                if (jsonLd && jsonLd['@type'] === 'JobPosting') {
                    crawlerLog.debug('Extracting data from JSON-LD.');
                    title = jsonLd.title;
                    company = jsonLd.hiringOrganization?.name;
                    date_posted = jsonLd.datePosted;
                    job_type = jsonLd.employmentType;
                    const { addressLocality, addressRegion } = jsonLd.jobLocation?.address || {};
                    location = [addressLocality, addressRegion].filter(Boolean).join(', ');
                    
                    // Extract salary from JSON-LD
                    if (jsonLd.baseSalary) {
                        const salaryValue = jsonLd.baseSalary.value || {};
                        if (salaryValue.minValue || salaryValue.maxValue) {
                            const currency = jsonLd.baseSalary.currency || 'NGN';
                            const min = salaryValue.minValue ? `${currency} ${salaryValue.minValue.toLocaleString()}` : '';
                            const max = salaryValue.maxValue ? `${currency} ${salaryValue.maxValue.toLocaleString()}` : '';
                            salary_range = [min, max].filter(Boolean).join(' - ');
                        }
                    }
                    
                    description_text = cleanText(cheerioLoad(jsonLd.description || '').text());
                    description_html = sanitizeDescription($, cheerioLoad(jsonLd.description || ''), request.url);
                } else {
                    // HTML selectors for Jobberman - using exact XPath conversions
                    crawlerLog.debug('Extracting data from HTML selectors using XPath mappings.');
                    
                    // Extract using precise XPath-converted selectors
                    const titleEl = getElementByPath($, 'title');
                    const companyEl = getElementByPath($, 'company');
                    const jobTypeEl = getElementByPath($, 'job_type');
                    const locationEl = getElementByPath($, 'location');
                    const salaryEl = getElementByPath($, 'salary');
                    const categoryEl = getElementByPath($, 'category');
                    const descriptionEl = getElementByPath($, 'description');
                    
                    // Extract complete text (not abbreviations)
                    title = cleanText(titleEl.text());
                    company = cleanText(companyEl.text());
                    job_type = cleanText(jobTypeEl.text());
                    location = cleanText(locationEl.text());
                    salary_range = cleanText(salaryEl.text());
                    category = cleanText(categoryEl.text());
                    
                    // Fallback selectors if XPath-converted selectors don't work
                    if (!title) {
                        title = cleanText($('h1').first().text());
                    }
                    if (!company) {
                        company = cleanText($('h2').first().text());
                    }
                    
                    // Date posted - look for "New", "Today", "Yesterday", "X days ago", etc.
                    const datePatterns = ['New', 'Today', 'Yesterday'];
                    const bodyText = $('body').text();
                    for (const pattern of datePatterns) {
                        if (bodyText.includes(pattern)) {
                            date_posted = pattern;
                            break;
                        }
                    }
                    if (!date_posted) {
                        const daysAgoMatch = bodyText.match(/(\d+)\s+days?\s+ago/i);
                        if (daysAgoMatch) {
                            date_posted = daysAgoMatch[0];
                        }
                    }
                    
                    // Description extraction from XPath element
                    if (descriptionEl.length > 0) {
                        description_html = sanitizeDescription($, descriptionEl, request.url);
                        description_text = cleanText(descriptionEl.text());
                    } else {
                        // Fallback: find Job Summary and Job Description sections
                        const descriptionSections = [];
                        
                        $('h3, h2, h4').each((_, heading) => {
                            const headingText = $(heading).text().trim();
                            if (headingText.match(/job\s+summary/i)) {
                                let nextElement = $(heading).next();
                                while (nextElement.length && !nextElement.is('h1, h2, h3, h4, h5, h6')) {
                                    if (nextElement.text().trim()) {
                                        descriptionSections.push(nextElement);
                                    }
                                    nextElement = nextElement.next();
                                }
                            }
                            if (headingText.match(/job\s+description|requirements/i)) {
                                let nextElement = $(heading).next();
                                while (nextElement.length && !nextElement.is('h1, h2, h3, h4, h5, h6')) {
                                    if (nextElement.text().trim()) {
                                        descriptionSections.push(nextElement);
                                    }
                                    nextElement = nextElement.next();
                                }
                            }
                        });
                        
                        if (descriptionSections.length > 0) {
                            const descContainer = $('<div></div>');
                            descriptionSections.forEach(el => {
                                descContainer.append($(el).clone());
                            });
                            description_html = sanitizeDescription($, descContainer, request.url);
                            description_text = cleanText(descContainer.text());
                        } else {
                            const container = findBestDescriptionContainer($);
                            description_html = sanitizeDescription($, container, request.url);
                            description_text = cleanText(container.text());
                        }
                    }
                }

                if (!title) {
                    crawlerLog.warning('Could not extract job title.', { url: request.url });
                    return; // Skip if essential data is missing
                }

                const item = {
                    url: request.url,
                    title,
                    company: company || null,
                    location: location || null,
                    job_type: job_type || null,
                    category: category || null,
                    salary_range: salary_range || null,
                    date_posted: date_posted || null,
                    description_html: description_html || null,
                    description_text: description_text || null,
                    _source: 'jobberman.com',
                    _fetchedAt: new Date().toISOString(),
                };

                await Dataset.pushData(item);
                jobsScraped++;
                scrapedUrls.add(request.url);
                crawlerLog.info(`✓ Progress: ${jobsScraped}/${RESULTS_WANTED} jobs saved. Title: ${title}`);
            } catch (e) {
                crawlerLog.error(`Error extracting job details from ${request.url}: ${e.message}`);
            }
        }
    },
    
    failedRequestHandler: async ({ request, session }, error) => {
        log.warning(`Request failed: ${request.url} - ${error.message}`);
        if (session && (error.message.includes('403') || error.message.includes('blocked'))) {
            session.retire();
            log.warning('Session retired due to blocking.');
        }
    },
});

try {
    await crawler.run(initialUrls.map(u => ({ url: u, userData: { label: 'LIST' } })));
    log.info(`✓ Scraping completed. Total jobs saved: ${jobsScraped}`);
    healthCheckPassed = true;
    
    if (jobsScraped === 0) {
        log.warning('No jobs were scraped. Check selectors or website structure.');
    }
    
    log.info(`Actor finished in ${Math.round((Date.now() - startTime) / 1000)} seconds.`);
} catch (error) {
    log.error(`Actor failed: ${error.message}`, { stack: error.stack });
} finally {
    healthCheckPassed = true; // Ensure exit
}

await Actor.exit();
