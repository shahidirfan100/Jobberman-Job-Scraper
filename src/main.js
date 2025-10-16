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
    // More specific selector to target job cards
    $('.job-card a, .job-listing a, article a').each((_, a) => {
        const href = $(a).attr('href');
        if (href && href.includes('/listings/')) {
            const abs = toAbs(href);
            if (abs) links.add(abs);
        }
    });
    return [...links];
};

const findNextUrl = ($, currentUrl) => {
    const nextLink = $('a[rel="next"]').attr('href');
    if (nextLink) return toAbs(nextLink);

    // Fallback: find current page and get next sibling
    const activePage = $('.pagination .active, .pagination .current').first();
    if (activePage.length) {
        const next = activePage.next().find('a').attr('href');
        if (next) return toAbs(next);
    }
    return null;
};

const findBestDescriptionContainer = ($) => {
    const selectors = [
        '.job-details__main', // Primary container on Jobberman
        '[class*="job-description"]',
        '#job-description',
        'article.job-details',
    ];
    for (const sel of selectors) {
        const el = $(sel).first();
        if (el.length) return el;
    }
    return $('body'); // Fallback
};

const cleanText = (text) => {
    return String(text || '')
        .replace(/[
	]+/g, ' ')
        .replace(/[^\u0020-\u007E\u00A0-\u024F\u1E00-\u1EFF]/g, '')
        .trim();
};

const sanitizeDescription = ($, el, baseUrl) => {
    if (!el || !el.length) return '';
    const clone = el.clone();
    
    clone.find('script, style, noscript, svg, button, form, header, footer, nav, aside').remove();
    clone.find('[class*="social"], [class*="share"], [class*="apply-now-button"]').remove();

    clone.find('*').each((_, node) => {
        const $node = $(node);
        const attribs = Object.keys(node.attribs || {});
        
        if (node.tagName === 'a') {
            const href = $node.attr('href');
            attribs.forEach(attr => $node.removeAttr(attr));
            if (href) {
                const abs = toAbs(href, baseUrl);
                if (abs) $node.attr('href', abs);
            }
        } else {
            attribs.forEach(attr => $node.removeAttr(attr));
        }
    });

    // Remove empty elements
    for (let i = 0; i < 3; i++) {
        const empties = clone.find('*').filter((_, n) => $(n).text().trim() === '' && $(n).children().length === 0);
        if (empties.length === 0) break;
        empties.remove();
    }

    const html = clone.html() || '';
    clone.remove();
    return html.replace(/\s+/g, ' ').trim();
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

            let title, company, location, date_posted, description_html, description_text;

            try {
                // Try JSON-LD first
                const jsonLdScript = $('script[type="application/ld+json"]').html();
                const jsonLd = jsonLdScript ? safeJsonParse(jsonLdScript) : null;

                if (jsonLd && jsonLd['@type'] === 'JobPosting') {
                    crawlerLog.debug('Extracting data from JSON-LD.');
                    title = jsonLd.title;
                    company = jsonLd.hiringOrganization?.name;
                    date_posted = jsonLd.datePosted;
                    const { addressLocality, addressRegion } = jsonLd.jobLocation?.address || {};
                    location = [addressLocality, addressRegion].filter(Boolean).join(', ');
                    description_text = cleanText(cheerioLoad(jsonLd.description || '').text());
                    description_html = sanitizeDescription($, cheerioLoad(jsonLd.description || ''), request.url);
                } else {
                    // Fallback to HTML selectors
                    crawlerLog.debug('Extracting data from HTML selectors.');
                    title = cleanText($('h1.job-header__title').first().text());
                    company = cleanText($('div.job-header__company > a').first().text());
                    location = cleanText($('span.job-location').first().text());
                    date_posted = cleanText($('span.job-post-date').first().text());
                    
                    const container = findBestDescriptionContainer($);
                    description_html = sanitizeDescription($, container, request.url);
                    description_text = cleanText(container.text());
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
