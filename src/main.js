'use strict';

// main.js — Jobberman scraper (Apify SDK + Crawlee + got-scraping)
// - Robust field extraction (no abbreviations)
// - Clean description_html (text-related tags only; keeps clean <a href>)
// - JSON-LD aware (multiple scripts, tolerant to arrays)
// - Resilient selectors + fallbacks + heuristics
// - Modern HTTP client (got-scraping via Crawlee)

const { Actor } = require('apify');
const { CheerioCrawler, RequestQueue, log } = require('crawlee');

// -------------------- Helpers --------------------
const cleanText = (s) => (s || '')
    .replace(/\s+/g, ' ')
    .replace(/[\u00A0\t\r\n]+/g, ' ')
    .trim();

const deEllipsize = (s) => (s || '').replace(/\u2026/g, '...');

const safeJsonParse = (raw) => {
    try {
        if (!raw) return null;
        return JSON.parse(raw);
    } catch (e) {
        try {
            // Sometimes JSON-LD is poorly escaped; attempt a mild clean
            const fixed = raw.replace(/\n/g, ' ').replace(/\t/g, ' ').replace(/\r/g, ' ');
            return JSON.parse(fixed);
        } catch (_) {
            return null;
        }
    }
};

const toAbs = (href, base) => {
    try {
        return new URL(href, base).toString();
    } catch (_) {
        return '';
    }
};

const getFullText = ($el) => {
    if (!$el || $el.length === 0) return '';
    const rich = $el.attr('title') || $el.attr('aria-label') || $el.attr('data-title') || '';
    return cleanText(rich || $el.text());
};

// Keep only text-related tags; strip attributes except <a href>, which we absolutize.
const sanitizeDescription = ($rootDoc, $fragment, baseUrl) => {
    const cheerio = require('cheerio');
    const $ = cheerio.load('<div id="__tmp"></div>');
    $('#__tmp').append($fragment.clone());

    const allowedTags = ['p', 'br', 'strong', 'b', 'em', 'i', 'u', 'ul', 'ol', 'li',
        'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'a', 'span', 'div', 'section', 'article'];

    $('#__tmp').find('*').each((_, node) => {
        const $node = $(node);
        const tagName = (node.tagName || '').toLowerCase();

        // Capture href BEFORE removing attributes
        const originalHref = tagName === 'a' ? ($node.attr('href') || '') : '';

        // Strip all attributes
        for (const attr of Object.keys(node.attribs || {})) $node.removeAttr(attr);

        // Re-apply a cleaned absolute href for anchors only
        if (tagName === 'a' && originalHref) {
            const abs = toAbs(originalHref, baseUrl);
            if (abs) $node.attr('href', abs);
        }

        // Remove non-allowed tags but keep their text content
        if (!allowedTags.includes(tagName)) {
            const text = $node.text();
            if (cleanText(text)) $node.replaceWith(text);
            else $node.remove();
        }
    });

    // Remove empty nodes (except <br>)
    $('#__tmp').find('*').each((_, node) => {
        const $node = $(node);
        if (node.tagName && node.tagName.toLowerCase() !== 'br') {
            if (!cleanText($node.text()) && $node.children().length === 0) $node.remove();
        }
    });

    return $('#__tmp').html();
};

// Map of “intent” XPaths to robust CSS selectors (primary + fallbacks)
const buildSelectorMap = () => ({
    title: [
        'article h1',
        'header h1',
        '.job-details h1',
        'h1[class*="job" i]'
    ],
    company: [
        'article h2:first-of-type',
        'header h2',
        '[class*="company" i] h2',
        '[itemprop="hiringOrganization"] [itemprop="name"]'
    ],
    job_type: [
        'article div a[href*="employment" i]',
        '[class*="employment" i] a',
        '[itemprop="employmentType"]',
        'a[href*="employment" i]:nth-of-type(2)'
    ],
    location: [
        'article div a[href*="location" i]',
        '[class*="location" i] a, [class*="job-" i] a:nth-of-type(1)',
        '[itemprop="jobLocation"] [itemprop*="addressLocality" i]',
    ],
    salary: [
        'article div [class*="salary" i] span',
        '[class*="salary" i]',
        '[itemprop="baseSalary"]'
    ],
    category: [
        'article div a[href*="category" i]',
        '[class*="category" i] a'
    ],
    description: [
        'article > div:nth-of-type(4)',
        '.job-description, .job-details__main, [class*="job-description" i], #job-description, article.job-details, .job-summary, .job-content',
        '[itemprop="description"]'
    ],
});

const pickFirst = ($, selectors) => {
    for (const sel of selectors) {
        const $el = $(sel).first();
        if ($el && $el.length) return $el;
    }
    return null;
};

const extractDatePosted = ($, baseScopeSel = 'article, header, .job-top, .job-details__header') => {
    const topText = cleanText($(baseScopeSel).first().text()) || cleanText($('body').text());
    if (!topText) return null;
    let m = topText.match(/\b(\d{1,2})\s+(hours?|days?)\s+ago\b/i);
    if (m) return m[0];
    m = topText.match(/\b(Today|Yesterday|New)\b/i);
    if (m) return m[0];
    // Fallback to ISO-like dates
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
            if (isJob) {
                jsonLd = node;
                return false;
            }
        }
    });
    return jsonLd;
};

const enrichFromJsonLd = (jsonLd, fields, baseUrl, cheerioLoad) => {
    if (!jsonLd) return fields;
    const out = { ...fields };

    out.title = jsonLd.title || out.title;
    out.company = (jsonLd.hiringOrganization && (jsonLd.hiringOrganization.name || jsonLd.hiringOrganization['@name'])) || out.company;
    out.date_posted = jsonLd.datePosted || out.date_posted;

    if (jsonLd.employmentType) {
        out.job_type = Array.isArray(jsonLd.employmentType)
            ? jsonLd.employmentType.join(', ')
            : jsonLd.employmentType;
    }

    // Location
    const locNode = jsonLd.jobLocation || jsonLd.jobLocationType;
    if (locNode && typeof locNode === 'object') {
        const addr = Array.isArray(locNode) ? locNode[0]?.address : locNode.address;
        if (addr) {
            const parts = [addr.addressLocality, addr.addressRegion, addr.addressCountry].filter(Boolean);
            if (parts.length) out.location = out.location || parts.join(', ');
        }
    }

    // Salary
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

    // Description (prefer HTML if present)
    if (jsonLd.description) {
        const descRaw = jsonLd.description;
        const cheerio = require('cheerio');
        const $desc = cheerio.load(`<div>${descRaw}</div>`);
        const fragment = $desc('div').first();
        const html = sanitizeDescription($desc, fragment, baseUrl);
        const text = cleanText(fragment.text());
        if (html && cleanText(text)) {
            out.description_html = html;
            out.description_text = text;
        }
    }

    return out;
};

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

const findNextPage = ($, baseUrl) => {
    const cheerio = require('cheerio');
    // 1) rel="next"
    let href = $('a[rel="next"]').attr('href');
    // 2) typical page param
    if (!href) href = $('a[href*="?page="]').filter((_, a) => /next/i.test($(a).text())).attr('href');
    // 3) generic "next" text
    if (!href) href = $('a').filter((_, a) => /next/i.test($(a).text())).attr('href');
    if (!href) return '';
    return toAbs(href, baseUrl);
};

// -------------------- Core extraction --------------------
const extractFromDetail = async ({ request, $, cheerio }) => {
    const sel = buildSelectorMap();

    let title = '', company = '', job_type = '', location = '', salary_range = '', category = '';
    let description_html = '', description_text = '', date_posted = '';

    // Title, company, etc. with robust selectors
    const $title = pickFirst($, sel.title);
    const $company = pickFirst($, sel.company);
    const $jobType = pickFirst($, sel.job_type);
    const $location = pickFirst($, sel.location);
    const $salary = pickFirst($, sel.salary);
    const $category = pickFirst($, sel.category);

    title = deEllipsize(getFullText($title));
    company = deEllipsize(getFullText($company));
    job_type = deEllipsize(getFullText($jobType));
    location = deEllipsize(getFullText($location));
    salary_range = deEllipsize(getFullText($salary));
    category = deEllipsize(getFullText($category));

    // Description primary
    let $desc = pickFirst($, sel.description);
    if ($desc && $desc.length) {
        description_html = sanitizeDescription($, $desc.clone(), request.url);
        description_text = cleanText($desc.text());
    }

    // Fallback via headings cluster
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

    // Heuristic biggest text block
    if (!description_text) {
        const $big = biggestTextBlockHeuristic($);
        if ($big) {
            description_html = sanitizeDescription($, $big.clone(), request.url) || description_html;
            description_text = cleanText($big.text()) || description_text;
        }
    }

    // Date posted from visible page
    date_posted = extractDatePosted($) || '';

    // JSON-LD enrichment
    const jsonLd = parseJsonLdJob($);
    const cheerioLoad = require('cheerio').load;
    ({ title, company, job_type, location, salary_range, category, description_html, description_text, date_posted } =
        enrichFromJsonLd(jsonLd, { title, company, job_type, location, salary_range, category, description_html, description_text, date_posted }, request.url, cheerioLoad));

    return {
        url: request.url,
        title,
        company,
        job_type,
        location,
        salary_range,
        category,
        description_html,
        description_text,
        date_posted,
    };
};

// Simple heuristic to decide if page is a detail page
const isDetailPage = ($) => {
    return $('article h1, header h1, .job-details h1, h1[class*="job" i]').length > 0 || $('script[type="application/ld+json"]').length > 0;
};

// -------------------- Actor --------------------
await Actor.init();

(async () => {
    const input = await Actor.getInput() || {};

    const {
        startUrls = [
            // Nigeria site listing as an example; pass your own in input
            { url: 'https://www.jobberman.com/jobs' },
        ],
        maxRequestsPerCrawl = 100,
        maxConcurrency = 5,
        proxyConfiguration = null,
        requestTimeoutSecs = 45,
        requestHandlerTimeoutSecs = 60,
    } = input;

    const requestQueue = await Actor.openRequestQueue();
    for (const u of startUrls) await requestQueue.addRequest({ url: u.url });

    const crawler = new CheerioCrawler({
        requestQueue,
        maxRequestsPerCrawl,
        maxConcurrency,
        // Crawlee uses got-scraping internally for HTTP; these options keep it modern/robust
        requestHandlerTimeoutSecs,
        // Respectful headers
        additionalMimeTypes: ['application/ld+json'],
        preNavigationHooks: [async ({ request, session, proxyInfo }, gotoOptions) => {
            // You can tweak headers or cookies here if needed
            gotoOptions.headers = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'accept-language': 'en-US,en;q=0.9',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
                'cache-control': 'no-cache',
                'pragma': 'no-cache',
            };
        }],
        useSessionPool: true,
        persistCookiesPerSession: true,
        retryOnBlocked: true,
        // Main handler
        requestHandler: async ({ request, $, enqueueLinks }) => {
            if (!$ || typeof $.html !== 'function') {
                log.warning(`No HTML parsed for ${request.url} — skipping.`);
                return;
            }

            if (isDetailPage($)) {
                const item = await extractFromDetail({ request, $, cheerio: require('cheerio') });

                // Ensure minimum quality: must have title & description
                if (cleanText(item.title) && cleanText(item.description_text)) {
                    await Actor.pushData(item);
                    log.info(`Saved job: ${item.title} — ${item.company || 'Unknown company'}`);
                } else {
                    log.warning(`Incomplete job skipped: ${request.url}`);
                }
            } else {
                // LIST page: enqueue details and try to pick pagination
                const before = requestQueue.handledCount + requestQueue.pendingCount();

                await enqueueLinks({
                    strategy: 'same-domain',
                    globs: [
                        // Detail pages often contain '/job/' or '/listing/' etc. Keep it wide but in-domain.
                        '**/jobs/*',
                        '**/job/*',
                    ],
                });

                // Manual capture if listing cards use anchors without '/job/':
                $('a').each((_, a) => {
                    const href = $(a).attr('href');
                    if (!href) return;
                    const abs = toAbs(href, request.url);
                    if (/jobberman\./i.test(abs) && /(job|jobs)\//i.test(abs)) requestQueue.addRequest({ url: abs }).catch(() => {});
                });

                const next = findNextPage($, request.url);
                if (next) await requestQueue.addRequest({ url: next, uniqueKey: next });

                const after = requestQueue.handledCount + requestQueue.pendingCount();
                log.info(`Queued ${(after - before)} links from listing: ${request.url}`);
            }
        },
        failedRequestHandler: async ({ request, error }) => {
            log.error(`Request failed ${request.url}: ${error && error.message}`);
        },
        // Timeouts at HTTP layer
        navigationTimeoutSecs: requestTimeoutSecs,
    });

    await crawler.run();

    log.info('Crawl finished.');
    await Actor.exit();
})();
