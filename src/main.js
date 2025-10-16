// Jobberman.com jobs scraper (CheerioCrawler)
// Runtime: Node.js 18+, ESM (package.json has "type":"module")
// Stacks: Apify SDK, Crawlee, modern HTTP (got-scraping via Crawlee)
// NOTE: We keep your structure and add robust extraction for job_type, location,
// salary_range & category from LIST and DETAIL pages, with JSON-LD + heuristics.

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

if (!input || typeof input !== 'object') {
    throw new Error('Invalid input: Input must be an object');
}

const {
    keyword = '',
    location: locationFilter = '',
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

const hasSearchTerms = keyword || locationFilter;
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
    if (Date.now() - lastActivityTime > 290000) { // ~4m50s
        log.error('Actor has not shown activity for a long time. Exiting to prevent stall.');
        process.exit(1);
    }
}, 60000);

// ------------------------- HELPERS -------------------------
const safeJsonParse = (str) => {
    try { return JSON.parse(str); } catch (err) { log.debug(`Bad JSON: ${err.message}`); return null; }
};

const USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:115.0) Gecko/20100101 Firefox/115.0',
];

const buildStartUrl = (kw, loc, date) => {
    const u = new URL('https://www.jobberman.com/jobs');
    if (kw && String(kw).trim()) u.searchParams.set('q', String(kw).trim());
    if (loc && String(loc).trim()) u.searchParams.set('l', String(loc).trim());
    const dateMap = { '24h': '1 day', '7d': '7 days', '30d': '30 days' };
    if (date && dateMap[date]) u.searchParams.set('created_at', dateMap[date]);
    return u.href;
};

const toAbs = (href, base = 'https://www.jobberman.com') => {
    try {
        const abs = new URL(href, base).href;
        return /^https?:/i.test(abs) ? abs : null;
    } catch { return null; }
};

const cleanText = (text) => String(text || '')
    .replace(/[\u00A0\t\r\n]+/g, ' ')
    .replace(/\s{2,}/g, ' ')
    .trim();

const deEllipsize = (s) => (s || '').replace(/\u2026/g, '...');

const getFullText = ($el) => {
    if (!$el || !$el.length) return '';
    const rich = $el.attr('title') || $el.attr('aria-label') || $el.attr('data-title') || '';
    return cleanText(rich || $el.text());
};

const normalizeCookieHeader = ({ cookies, cookiesJson }) => {
    if (cookies && typeof cookies === 'string' && cookies.trim()) return cookies.trim();
    if (cookiesJson && typeof cookiesJson === 'string') {
        try {
            const parsed = JSON.parse(cookiesJson);
            if (Array.isArray(parsed)) return parsed.map(c => `${c.name}=${c.value}`).join('; ');
            if (typeof parsed === 'object') return Object.entries(parsed).map(([k, v]) => `${k}=${v}`).join('; ');
        } catch (e) { log.debug('Could not parse cookiesJson', { error: e.message }); }
    }
    return '';
};

// ------------------------- SELECTORS & JSON-LD -------------------------
const buildSelectorMap = () => ({
    title: [ 'article h1', 'header h1', '.job-details h1', 'h1[class*="job" i]' ],
    company: [ 'article h2:first-of-type', 'header h2', '[class*="company" i] h2', '[itemprop="hiringOrganization"] [itemprop="name"]' ],
    job_type: [ 'article div a[href*="employment" i]', '[class*="employment" i] a', '[itemprop="employmentType"]', 'a[href*="employment" i]:nth-of-type(2)' ],
    location: [ 'article div a[href*="location" i]', '[class*="location" i] a, [class*="job-" i] a:nth-of-type(1)', '[itemprop="jobLocation"] [itemprop*="addressLocality" i]' ],
    salary: [ 'article div [class*="salary" i] span', '[class*="salary" i]', '[itemprop="baseSalary"]' ],
    category: [ 'article div a[href*="category" i]', '[class*="category" i] a' ],
    description: [ 'article > div:nth-of-type(4)', '.job-description, .job-details__main, [class*="job-description" i], #job-description, article.job-details, .job-summary, .job-content', '[itemprop="description"]' ],
});

const pickFirst = ($, selectors) => {
    for (const sel of selectors) { const $el = $(sel).first(); if ($el && $el.length) return $el; }
    return null;
};

const extractDatePosted = ($, baseScopeSel = 'article, header, .job-top, .job-details__header') => {
    const topText = cleanText($(baseScopeSel).first().text()) || cleanText($('body').text());
    if (!topText) return null;
    let m = topText.match(/\b(\d{1,2})\s+(hours?|days?)\s+ago\b/i);
    if (m) return m[0];
    m = topText.match(/\b(Today|Yesterday|New)\b/i);
    if (m) return m[0];
    m = topText.match(/\b\d{4}-\d{2}-\d{2}\b/);
    if (m) return m[0];
    return null;
};

const parseJsonLdJob = ($) => {
    let jsonLd = null;
    $('script[type="application/ld+json"]').each((_, s) => {
        const raw = $(s).contents().text();
        const parsed = safeJsonParse(raw);
        if (!parsed) return;
        const arr = Array.isArray(parsed) ? parsed : [parsed];
        for (const node of arr) {
            if (!node) continue;
            const type = node['@type'];
            const isJob = type === 'JobPosting' || (Array.isArray(type) && type.includes('JobPosting'));
            if (isJob) { jsonLd = node; return false; }
        }
    });
    return jsonLd;
};

const enrichFromJsonLd = (jsonLd, fields, baseUrl) => {
    if (!jsonLd) return fields;
    const out = { ...fields };
    out.title = jsonLd.title || out.title;
    out.company = (jsonLd.hiringOrganization && (jsonLd.hiringOrganization.name || jsonLd.hiringOrganization['@name'])) || out.company;
    out.date_posted = jsonLd.datePosted || out.date_posted;

    if (jsonLd.employmentType) {
        out.job_type = Array.isArray(jsonLd.employmentType) ? jsonLd.employmentType.join(', ') : jsonLd.employmentType;
    }
    const locNode = jsonLd.jobLocation || jsonLd.jobLocationType;
    if (locNode && typeof locNode === 'object') {
        const addr = Array.isArray(locNode) ? locNode[0]?.address : locNode.address;
        if (addr) {
            const parts = [addr.addressLocality, addr.addressRegion, addr.addressCountry].filter(Boolean);
            if (parts.length) out.location = out.location || parts.join(', ');
        }
    }
    const sal = jsonLd.baseSalary;
    if (sal) {
        const currency = sal.currency || sal.value?.currency || 'NGN';
        const val = sal.value || sal;
        const min = (val && (val.minValue ?? val.value ?? val.amount)) || '';
        const max = (val && (val.maxValue ?? '')) || '';
        const mk = (n) => (n === '' ? '' : `${currency} ${Number(n).toLocaleString()}`);
        const range = [mk(min), mk(max)].filter(Boolean).join(' - ');
        out.salary_range = range || out.salary_range;
    }
    if (jsonLd.industry && !out.category) out.category = Array.isArray(jsonLd.industry) ? jsonLd.industry[0] : jsonLd.industry;

    if (jsonLd.description) {
        const $wrapper = cheerioLoad(`<div>${jsonLd.description}</div>`);
        const frag = $wrapper('div').first();
        out.description_html = sanitizeDescription($wrapper, frag, baseUrl) || out.description_html;
        out.description_text = cleanText(frag.text()) || out.description_text;
    }
    return out;
};

// ------------------------- DESCRIPTION SANITIZER -------------------------
const sanitizeDescription = ($, el, baseUrl) => {
    if (!el || !el.length) return '';
    const clone = el.clone();
    clone.find('script, style, noscript, svg, button, form, header, footer, nav, aside, iframe').remove();
    clone.find('[class*="social" i], [class*="share" i], [class*="apply" i], [class*="login" i]').remove();
    clone.find('[class*="banner" i], [class*="ad-" i], [class*="advert" i]').remove();
    clone.find('[id*="ad-" i], [id*="banner" i], [class*="cookie" i]').remove();

    const allowed = ['p','br','strong','b','em','i','u','ul','ol','li','h1','h2','h3','h4','h5','h6','a','span','div','section','article'];

    // Capture <a href> before stripping attributes, then restore absolute href
    clone.find('*').each((_, node) => {
        const $node = cheerioLoad(node);
        const tagName = (node.tagName || '').toLowerCase();
        const originalHref = tagName === 'a' ? ($node.attr('href') || '') : '';
        // remove attributes
        Object.keys(node.attribs || {}).forEach(attr => $node.removeAttr(attr));
        if (tagName === 'a' && originalHref) {
            const abs = toAbs(originalHref, baseUrl);
            if (abs) $node.attr('href', abs);
        }
        if (!allowed.includes(tagName)) {
            const t = $node.text();
            if (cleanText(t)) $node.replaceWith(t); else $node.remove();
        }
    });

    // Remove empty nodes (except br)
    clone.find('*').each((_, node) => {
        const $n = cheerioLoad(node);
        const tag = (node.tagName || '').toLowerCase();
        if (tag !== 'br') {
            if (!cleanText($n.text()) && $n.children().length === 0) $n.remove();
        }
    });

    let html = clone.html() || '';
    html = html.replace(/>\s+</g, '><').replace(/\s{2,}/g, ' ').trim();
    return html;
};

// ------------------------- LIST PAGE PARSING -------------------------
const collectJobLinks = ($) => {
    const links = new Set();
    $('a[href*="/listings/"]').each((_, a) => {
        const href = $(a).attr('href');
        if (!href) return;
        const abs = toAbs(href);
        if (abs && !abs.includes('#')) links.add(abs.split('?')[0]);
    });
    // Fallbacks (keep original breadth)
    if (links.size === 0) {
        $('.job-list li a, .job-card a, .search-result-item a, .job-item a').each((_, a) => {
            const href = $(a).attr('href');
            if (!href) return;
            const abs = toAbs(href);
            if (abs) links.add(abs.split('?')[0]);
        });
    }
    return [...links];
};

const findNextUrl = ($, currentUrl) => {
    const nextLink = $('a[href*="?page="]').filter((_, a) => /next/i.test($(a).text())).attr('href') ||
                     $('a[rel="next"]').attr('href') ||
                     $('a').filter((_, a) => /Go to next page/i.test($(a).text())).attr('href');
    if (nextLink) return toAbs(nextLink);
    const m = currentUrl.match(/[?&]page=(\d+)/);
    if (m) return currentUrl.replace(/([?&])page=\d+/, `$1page=${parseInt(m[1], 10) + 1}`);
    return currentUrl.includes('?') ? `${currentUrl}&page=2` : `${currentUrl}?page=2`;
};

// Heuristic extraction from a LISTING card block (additive, robust for job_type/location/salary/category)
const extractFromListingCard = ($, $card) => {
    const result = { title: '', company: '', location: '', job_type: '', salary_range: '', category: '' };

    const $titleA = $card.find('a[href*="/listings/"]').first();
    result.title = cleanText($titleA.text());

    // Collect candidate lines
    const lines = cleanText($card.text()).split(/\s{2,}|\n+/).map(s => s.trim()).filter(Boolean);

    // Company: the first non-title, non-meta line near the top
    for (const line of lines) {
        if (line === result.title) continue;
        if (/^(New|Today|Yesterday)$/i.test(line)) continue;
        if (/\b(Easy Apply|FEATURED)\b/i.test(line)) continue;
        if (/\b(NGN|GHS|KES|ZAR|USD)\b\s*\d/i.test(line)) continue; // salary-like
        if (/\b(Full\s*Time|Part\s*Time|Contract|Temporary|Internship|Remote|Hybrid|Freelance|Volunteer)\b/i.test(line)) continue;
        result.company = line; break;
    }

    // A single line often contains: LOCATION  JOB_TYPE  SALARY
    const metaLine = lines.find(l => /\b(Full\s*Time|Part\s*Time|Contract|Temporary|Internship|Remote|Hybrid|Freelance|Volunteer)\b/i.test(l));
    if (metaLine) {
        const jtMatch = metaLine.match(/\b(Full\s*Time|Part\s*Time|Contract|Temporary|Internship|Remote|Hybrid|Freelance|Volunteer)\b/i);
        if (jtMatch) result.job_type = jtMatch[1].replace(/\s+/g, ' ');
        const locPart = metaLine.split(jtMatch ? jtMatch[0] : '')[0];
        if (locPart) result.location = cleanText(locPart);
    }

    // Salary: NGN 75,000 - 150,000 (or similar)
    const salLine = lines.find(l => /\b([A-Z]{3})\s*\d[\d,]*(?:\s*(?:-|to)\s*([A-Z]{3})?\s*[\d,]+)?/i.test(l));
    if (salLine) {
        const m = salLine.match(/\b([A-Z]{3})\s*([\d,]+)(?:\s*(?:-|to)\s*([A-Z]{3})?\s*([\d,]+))?/i);
        if (m) {
            const c1 = m[1] || m[3] || 'NGN';
            const min = Number(m[2].replace(/,/g, '')); const max = m[4] ? Number(m[4].replace(/,/g, '')) : null;
            result.salary_range = max ? `${c1} ${min.toLocaleString()} - ${c1} ${max.toLocaleString()}` : `${c1} ${min.toLocaleString()}`;
        }
    }

    // Category: a standalone line near the meta/salary that is not company/title nor date badges
    const catIdx = lines.findIndex(l => ![result.title, result.company].includes(l) && !/\b(New|Today|Yesterday|Easy Apply|FEATURED)\b/i.test(l) && !/\b(NGN|GHS|KES|ZAR|USD)\b\s*\d/i.test(l) && !/\b(Full\s*Time|Part\s*Time|Contract|Temporary|Internship|Remote|Hybrid|Freelance|Volunteer)\b/i.test(l));
    if (catIdx > -1) result.category = lines[catIdx];

    return result;
};

// ------------------------- DETAIL PAGE EXTRACTION -------------------------
const biggestTextBlockHeuristic = ($) => {
    const candidates = [];
    $('main, article').find('section, div, article').each((_, n) => {
        const $n = $(n);
        const txt = cleanText($n.text() || '');
        if (txt.length > 400) candidates.push({ $n, len: txt.length });
    });
    candidates.sort((a, b) => b.len - a.len);
    return candidates.length ? candidates[0].$n : null;
};

const extractFromDetail = async ({ request, $, cheerio }) => {
    const sel = buildSelectorMap();

    // Seed from LIST page if available
    let seed = request.userData?.seed || {};
    let title = seed.title || '', company = seed.company || '', job_type = seed.job_type || '', location = seed.location || '', salary_range = seed.salary_range || '', category = seed.category || '';
    let description_html = '', description_text = '', date_posted = '';

    const $title = pickFirst($, sel.title);
    const $company = pickFirst($, sel.company);
    const $jobType = pickFirst($, sel.job_type);
    const $location = pickFirst($, sel.location);
    const $salary = pickFirst($, sel.salary);
    const $category = pickFirst($, sel.category);

    title = deEllipsize(getFullText($title)) || title;
    company = deEllipsize(getFullText($company)) || company;
    job_type = deEllipsize(getFullText($jobType)) || job_type;
    location = deEllipsize(getFullText($location)) || location;
    salary_range = deEllipsize(getFullText($salary)) || salary_range;
    category = deEllipsize(getFullText($category)) || category;

    let $desc = pickFirst($, sel.description);
    if ($desc && $desc.length) {
        description_html = sanitizeDescription($, $desc.clone(), request.url);
        description_text = cleanText($desc.text());
    }
    if (!description_text) {
        const heading = $('h1:contains("Description"), h2:contains("Description"), h3:contains("Description")').first();
        if (heading && heading.length) {
            let $cand = heading.parent();
            if ($cand && $cand.length) {
                description_html = sanitizeDescription($, $cand.clone(), request.url) || description_html;
                description_text = cleanText($cand.text()) || description_text;
            }
        }
    }
    if (!description_text) {
        const $big = biggestTextBlockHeuristic($);
        if ($big) {
            description_html = sanitizeDescription($, $big.clone(), request.url) || description_html;
            description_text = cleanText($big.text()) || description_text;
        }
    }

    date_posted = extractDatePosted($) || '';

    const jsonLd = parseJsonLdJob($);
    ({ title, company, job_type, location, salary_range, category, description_html, description_text, date_posted } =
        enrichFromJsonLd(jsonLd, { title, company, job_type, location, salary_range, category, description_html, description_text, date_posted }, request.url));

    return { url: request.url, title, company, job_type, location, salary_range, category, description_html, description_text, date_posted };
};

const isDetailPage = ($) => {
    return $('article h1, header h1, .job-details h1, h1[class*="job" i]').length > 0 || $('script[type="application/ld+json"]').length > 0;
};

// ------------------------- START URLS -------------------------
const builtStartUrl = buildStartUrl(keyword, locationFilter, posted_date);
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
    maxRequestsPerMinute: 120,
    requestHandlerTimeoutSecs: 45,
    navigationTimeoutSecs: 45,
    maxConcurrency: 5,
    useSessionPool: true,
    persistCookiesPerSession: true,
    sessionPoolOptions: { maxPoolSize: 50, sessionOptions: { maxUsageCount: 50, maxErrorScore: 3 } },
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

            // Build per-card seeds for job_type/location/salary_range/category
            const seedsByUrl = new Map();
            $('a[href*="/listings/"]').each((_, a) => {
                const href = $(a).attr('href');
                const abs = toAbs(href, request.url);
                if (!abs) return;
                const $card = $(a).closest('li, article, .search-result-item, .job-card, .job-item, .search-result, div');
                const seed = extractFromListingCard($, $card);
                if (seed.title) seedsByUrl.set(abs.split('?')[0], seed);
            });

            if (!collectDetails) {
                const toPush = links.slice(0, RESULTS_WANTED - jobsScraped);
                if (toPush.length > 0) {
                    const items = toPush.map(u => ({ url: u, ...(seedsByUrl.get(u) || {}), _source: 'jobberman.com' }));
                    await Dataset.pushData(items);
                    jobsScraped += items.length;
                }
            } else {
                const toEnqueue = links.slice(0, RESULTS_WANTED - jobsEnqueued);
                for (const u of toEnqueue) {
                    await enqueueLinks({ urls: [u], userData: { label: 'DETAIL', seed: seedsByUrl.get(u) || {} } });
                    jobsEnqueued++;
                }
            }

            if (jobsScraped >= RESULTS_WANTED || jobsEnqueued >= RESULTS_WANTED || pageNo >= MAX_PAGES) {
                crawlerLog.info('Stopping pagination based on limits.');
                return;
            }

            const nextUrl = findNextUrl($, request.url);
            if (nextUrl) {
                crawlerLog.info(`Enqueuing next list page: ${pageNo + 1}`);
                await enqueueLinks({ urls: [nextUrl], userData: { label: 'LIST', pageNo: pageNo + 1 }, forefront: true });
            } else {
                crawlerLog.info(`No next page found on page ${pageNo}.`);
            }
        }

        if (label === 'DETAIL') {
            if (jobsScraped >= RESULTS_WANTED || scrapedUrls.has(request.url)) return;
            try {
                // Prefer DETAIL extraction, enriched by JSON-LD
                const item = await extractFromDetail({ request, $, cheerio: cheerioLoad });
                if (!cleanText(item.title)) { crawlerLog.warning('Missing title, skipping.'); return; }
                if (!cleanText(item.description_text)) {
                    // allow pushing even if description is thin, but keep structure
                    crawlerLog.warning('Description thin or missing; still saving core fields.');
                }
                await Dataset.pushData(item);
                jobsScraped++;
                scrapedUrls.add(request.url);
                crawlerLog.info(`✓ Progress: ${jobsScraped}/${RESULTS_WANTED} — ${item.title}`);
            } catch (e) {
                crawlerLog.error(`Error extracting job details from ${request.url}: ${e.message}`);
            }
        }
    },

    failedRequestHandler: async ({ request, session }, error) => {
        log.warning(`Request failed: ${request.url} - ${error.message}`);
        if (session && (String(error?.message || '').includes('403') || String(error?.message || '').includes('blocked'))) {
            session.retire();
            log.warning('Session retired due to blocking.');
        }
    },
});

try {
    await crawler.run(initialUrls.map(u => ({ url: u, userData: { label: 'LIST' } })));
    log.info(`✓ Scraping completed. Total jobs saved: ${jobsScraped}`);
    healthCheckPassed = true;
    if (jobsScraped === 0) log.warning('No jobs were scraped. Check selectors or website structure.');
    log.info(`Actor finished in ${Math.round((Date.now() - startTime) / 1000)} seconds.`);
} catch (error) {
    log.error(`Actor failed: ${error.message}`, { stack: error.stack });
} finally {
    healthCheckPassed = true;
}

await Actor.exit();
