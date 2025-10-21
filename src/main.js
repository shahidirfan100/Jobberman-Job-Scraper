// Jobberman.com jobs scraper (CheerioCrawler)
// ESM compatible (package.json has "type":"module")
// Stacks: Apify SDK, Crawlee, HTTP (got-scraping via Crawlee)

import { Actor, log } from 'apify';
import { CheerioCrawler, Dataset } from 'crawlee';
import { load as cheerioLoad } from 'cheerio';

// ------------------------- UTILITIES -------------------------
const cleanText = (s) => String(s ?? '')
  .replace(/[\u00A0\t\r\n]+/g, ' ')
  .replace(/\s{2,}/g, ' ')
  .trim();

const deEllipsize = (s) => (s || '').replace(/\u2026/g, '...');

const toAbs = (href, base = 'https://www.jobberman.com') => {
  try { return new URL(href, base).href; } catch { return null; }
};

const safeJsonParse = (raw) => {
  try { return JSON.parse(raw); } catch { return null; }
};

const getFullText = ($el) => {
  if (!$el || !$el.length) return '';
  const rich = $el.attr('title') || $el.attr('aria-label') || $el.attr('data-title') || '';
  return cleanText(rich || $el.text());
};

// Keep only text-related tags; strip all attributes **without** using removeAttr
const sanitizeDescription = ($ctx, $fragment, baseUrl) => {
  if (!$fragment || !$fragment.length) return '';
  // Work on a dedicated cheerio root to avoid side-effects
  const $ = cheerioLoad('<div id="__root"></div>');
  $('#__root').append($fragment.clone());

  const allowed = new Set([
    'p','br','strong','b','em','i','u','ul','ol','li',
    'h1','h2','h3','h4','h5','h6','a','span','div','section','article'
  ]);

  $('#__root').find('*').each((_, el) => {
    // Cheerio element nodes have type === 'tag' and the tag name in el.name
    const isTag = el && el.type === 'tag';
    const tag = isTag ? String(el.name || '').toLowerCase() : '';
    if (!isTag) return;

    // Preserve only absolute href for anchors
    const isAnchor = tag === 'a';
    let href = isAnchor ? (el.attribs?.href || '') : '';

    // Reset attributes safely
    el.attribs = {};
    if (isAnchor && href) {
      const abs = toAbs(href, baseUrl);
      if (abs) el.attribs.href = abs;
    }

    // Drop non-allowed tags but keep their text content
    if (!allowed.has(tag)) {
      const $el = $(el);
      const txt = cleanText($el.text());
      if (txt) $el.replaceWith(txt);
      else $el.remove();
    }
  });

  // Remove empty nodes (except <br>)
  $('#__root').find('*').each((_, el) => {
    if (!el || el.type !== 'tag') return;
    const tag = (el.name || '').toLowerCase();
    if (tag === 'br') return;
    const $el = $(el);
    if (!cleanText($el.text()) && $el.children().length === 0) $el.remove();
  });

  let html = $('#__root').html() || '';
  html = html.replace(/>\s+</g, '><').replace(/\s{2,}/g, ' ').trim();
  return html;
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
  // Handle tolerant date parsing
  const dateMap = { '24h': '1 day', '7d': '7 days', '30d': '30 days', '1 day': '1 day', '7 days': '7 days', '30 days': '30 days' };
  const normalizedDate = String(date).toLowerCase().replace(/\s+/g, '');
  if (dateMap[normalizedDate]) u.searchParams.set('created_at', dateMap[normalizedDate]);
  return u.href;
};

const normalizeCookieHeader = ({ cookies, cookiesJson }) => {
  if (cookies && typeof cookies === 'string' && cookies.trim()) return cookies.trim();
  if (cookiesJson && typeof cookiesJson === 'string') {
    try {
      const parsed = JSON.parse(cookiesJson);
      if (Array.isArray(parsed)) return parsed.map(c => `${c.name}=${c.value}`).join('; ');
      if (typeof parsed === 'object') return Object.entries(parsed).map(([k,v]) => `${k}=${v}`).join('; ');
    } catch (e) {
      log.warning(`Could not parse cookiesJson: ${e.message}`);
    }
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
  for (const sel of selectors) {
    const $el = $(sel).first();
    if ($el && $el.length) return $el;
  }
  return null;
};

const extractDatePosted = ($, scope = 'article, header, .job-top, .job-details__header') => {
  const txt = cleanText($(scope).first().text()) || cleanText($('body').text());
  if (!txt) return null;
  let m = txt.match(/\b(\d{1,2})\s+(hours?|days?)\s+ago\b/i);
  if (m) return m[0];
  m = txt.match(/\b(Today|Yesterday|New)\b/i);
  if (m) return m[0];
  m = txt.match(/\b\d{4}-\d{2}-\d{2}\b/);
  if (m) return m[0];
  return null;
};

const parseJsonLdJob = ($) => {
  let job = null;
  $('script[type="application/ld+json"]').each((_, s) => {
    const raw = $(s).contents().text();
    const parsed = safeJsonParse(raw);
    if (!parsed) return;
    
    // Handle @graph structure
    let candidates = [];
    if (parsed['@graph'] && Array.isArray(parsed['@graph'])) {
      candidates = parsed['@graph'];
    } else {
      candidates = Array.isArray(parsed) ? parsed : [parsed];
    }
    
    for (const node of candidates) {
      const t = node && node['@type'];
      const isJob = t === 'JobPosting' || (Array.isArray(t) && t.includes('JobPosting'));
      if (isJob) { 
        job = node; 
        return false; // Break out of .each()
      }
    }
  });
  return job;
};

const enrichFromJsonLd = (jsonLd, fields, baseUrl) => {
  if (!jsonLd) return fields;
  const out = { ...fields };
  
  // Title
  out.title = jsonLd.title || out.title;
  
  // Company
  out.company = (jsonLd.hiringOrganization && (jsonLd.hiringOrganization.name || jsonLd.hiringOrganization['@name'])) || out.company;
  
  // Date Posted
  out.date_posted = jsonLd.datePosted || out.date_posted;

  // Employment Type - handle FULL_TIME, PART_TIME, etc.
  if (jsonLd.employmentType) {
    const empType = Array.isArray(jsonLd.employmentType) ? jsonLd.employmentType[0] : jsonLd.employmentType;
    // Convert FULL_TIME to "Full Time", PART_TIME to "Part Time"
    out.job_type = empType.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
  }

  // Location - handle multiple formats
  const locNode = jsonLd.jobLocation || jsonLd.jobLocationType;
  if (locNode && typeof locNode === 'object') {
    const addr = Array.isArray(locNode) ? locNode[0]?.address : locNode.address;
    if (addr) {
      // Check for streetAddress (Jobberman uses this for city/state)
      if (addr.streetAddress) {
        out.location = addr.streetAddress;
      } else {
        // Fallback to locality/region/country
        const parts = [addr.addressLocality, addr.addressRegion, addr.addressCountry].filter(Boolean);
        if (parts.length) out.location = out.location || parts.join(', ');
      }
    }
  }

  // Salary - handle nested value structure
  const sal = jsonLd.baseSalary;
  if (sal) {
    const currency = sal.currency || sal.value?.currency || 'NGN';
    const val = sal.value || sal;
    const min = (val && (val.minValue ?? val.value ?? val.amount)) || '';
    const max = (val && (val.maxValue ?? '')) || '';
    
    if (min || max) {
      const mk = (n) => (n === '' ? '' : `${currency} ${Number(n).toLocaleString()}`);
      const range = [mk(min), mk(max)].filter(Boolean).join(' - ');
      out.salary_range = range || out.salary_range;
    }
  }

  // Category - use occupationalCategory or industry
  if (jsonLd.occupationalCategory && !out.category) {
    out.category = Array.isArray(jsonLd.occupationalCategory) ? jsonLd.occupationalCategory[0] : jsonLd.occupationalCategory;
  } else if (jsonLd.industry && !out.category) {
    out.category = Array.isArray(jsonLd.industry) ? jsonLd.industry[0] : jsonLd.industry;
  }

  // Description
  if (jsonLd.description) {
    const $wrap = cheerioLoad(`<div>${jsonLd.description}</div>`);
    const frag = $wrap('div').first();
    out.description_html = sanitizeDescription($wrap, frag, baseUrl) || out.description_html;
    out.description_text = cleanText(frag.text()) || out.description_text;
  }
  
  return out;
};

// ------------------------- LISTING PAGE HELPERS -------------------------
const collectJobLinks = ($, base) => {
  const links = new Set();
  // Primary pattern: /listings/<slug>
  $('a[href*="/listings/"]').each((_, a) => {
    const href = $(a).attr('href');
    const abs = href && toAbs(href, base);
    if (abs && !abs.includes('#')) links.add(abs.split('?')[0]);
  });
  // Fallbacks
  if (links.size === 0) {
    $('.job-list li a, .job-card a, .search-result-item a, .job-item a').each((_, a) => {
      const href = $(a).attr('href');
      const abs = href && toAbs(href, base);
      if (abs) links.add(abs.split('?')[0]);
    });
  }
  return [...links];
};

const findNextUrl = ($, currentUrl) => {
  const nextLink =
    $('a[href*="?page="]').filter((_, a) => /next/i.test($(a).text())).attr('href') ||
    $('a[rel="next"]').attr('href') ||
    $('a').filter((_, a) => /Go to next page/i.test($(a).text())).attr('href');
  if (nextLink) return toAbs(nextLink, currentUrl);
  
  // Fallback for page=N increment
  const m = currentUrl.match(/[?&]page=(\d+)/);
  if (m) return currentUrl.replace(/([?&])page=\d+/, `$1page=${parseInt(m[1], 10) + 1}`);
  // Fallback if no page=N is present
  return currentUrl.includes('?') ? `${currentUrl}&page=2` : `${currentUrl}?page=2`;
};

// Parse a listing card to seed fields
const extractFromListingCard = ($, $card) => {
  const res = { title: '', company: '', location: '', job_type: '', salary_range: '', category: '' };
  const $titleA = $card.find('a[href*="/listings/"]').first();
  res.title = cleanText($titleA.text());

  // Get all text nodes, split by newlines or multiple spaces, trim, and filter empties
  const lines = cleanText($card.text()).split(/\s{2,}|\n+/).map(s => s.trim()).filter(Boolean);

  // company
  for (const line of lines) {
    if (line === res.title) continue;
    if (/^(New|Today|Yesterday)$/i.test(line)) continue;
    if (/\b(Easy Apply|FEATURED)\b/i.test(line)) continue;
    if (/\b(NGN|GHS|KES|ZAR|USD)\b\s*\d/i.test(line)) continue;
    if (/\b(Full\s*Time|Part\s*Time|Contract|Temporary|Internship|Remote|Hybrid|Freelance|Volunteer)\b/i.test(line)) continue;
    // First line that is not title, date, badge, salary, or job type is likely company
    res.company = line; break; 
  }

  // meta: "LOCATION  JOB_TYPE  SALARY"
  const meta = lines.find(l => /\b(Full\s*Time|Part\s*Time|Contract|Temporary|Internship|Remote|Hybrid|Freelance|Volunteer)\b/i.test(l));
  if (meta) {
    const jt = meta.match(/\b(Full\s*Time|Part\s*Time|Contract|Temporary|Internship|Remote|Hybrid|Freelance|Volunteer)\b/i);
    if (jt) res.job_type = jt[1].replace(/\s+/g, ' ');
    // Location is everything before the job type
    const locPart = meta.split(jt ? jt[0] : '')[0];
    if (locPart) res.location = cleanText(locPart);
  }

  // salary
  const salLine = lines.find(l => /\b([A-Z]{3})\s*\d[\d,]*(?:\s*(?:-|to)\s*([A-Z]{3})?\s*[\d,]+)?/i.test(l));
  if (salLine) {
    const m = salLine.match(/\b([A-Z]{3})\s*([\d,]+)(?:\s*(?:-|to)\s*([A-Z]{3})?\s*([\d,]+))?/i);
    if (m) {
      const c = m[1] || m[3] || 'NGN';
      const min = Number(m[2].replace(/,/g, ''));
      const max = m[4] ? Number(m[4].replace(/,/g, '')) : null;
      res.salary_range = max ? `${c} ${min.toLocaleString()} - ${c} ${max.toLocaleString()}` : `${c} ${min.toLocaleString()}`;
    }
  }

  // category — first leftover non-badge/meta/salary line
  const catIdx = lines.findIndex(l =>
    ![res.title, res.company].includes(l) &&
    !/\b(New|Today|Yesterday|Easy Apply|FEATURED)\b/i.test(l) &&
    !/\b(NGN|GHS|KES|ZAR|USD)\b\s*\d/i.test(l) &&
    !/\b(Full\s*Time|Part\s*Time|Contract|Temporary|Internship|Remote|Hybrid|Freelance|Volunteer)\b/i.test(l)
  );
  if (catIdx > -1) res.category = lines[catIdx];

  return res;
};

// ------------------------- DETAIL EXTRACTION -------------------------
const biggestTextBlockHeuristic = ($) => {
  const candidates = [];
  // Look in main containers
  $('main, article').find('section, div, article').each((_, n) => {
    const $n = $(n);
    // Direct text content, ignoring children's text
    const directText = ($n.contents().filter((i, el) => el.type === 'text').text() || '');
    const txt = cleanText(directText);
    
    // Heuristic:
    // - Must have significant direct text (e.g., > 400 chars)
    // - Should not contain common list/card containers (lowers score)
    // - Should not be a tiny wrapper (lowers score)
    let score = txt.length;
    if ($n.find('ul, ol, a[href*="/listings/"], .job-card').length > 0) score *= 0.1;
    if ($n.children().length > 20) score *= 0.5; // Too many children, likely a wrapper
    
    if (score > 300) { // Lowered threshold for score
        candidates.push({ $n, score, len: txt.length });
    }
  });
  
  // Also check all elements and find the one with the most text
  $('body').find('*').each((_, n) => {
     const $n = $(n);
     const txt = cleanText($n.text() || '');
     if (txt.length > 400 && $n.children().length < 10) { // Prefer nodes with fewer children
         candidates.push({ $n, score: txt.length, len: txt.length });
     }
  });

  candidates.sort((a, b) => b.score - a.score);
  return candidates.length ? candidates[0].$n : null;
};

const extractFromDetail = ({ request, $ }) => {
  const sel = buildSelectorMap();
  let seed = request.userData?.seed || {};
  let title = seed.title || '', company = seed.company || '', job_type = seed.job_type || '', location = seed.location || '', salary_range = seed.salary_range || '', category = seed.category || '';
  let description_html = '', description_text = '', date_posted = '';

  // direct selectors
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

  // description
  let $desc = pickFirst($, sel.description);
  if ($desc && $desc.length) {
    description_html = sanitizeDescription($, $desc.clone(), request.url);
    description_text = cleanText($desc.text());
  }
  // Fallback 1: Heading-based
  if (!description_text || description_text.length < 100) {
    const heading = $('h1:contains("Description"), h2:contains("Description"), h3:contains("Description"), h4:contains("Description")').first();
    if (heading && heading.length) {
      // Try to find the content block *after* the heading
      let $cand = heading.nextAll('div, section').first();
      if (!$cand.length) $cand = heading.parent(); // Fallback to parent
      
      if ($cand && $cand.length) {
        const cand_html = sanitizeDescription($, $cand.clone(), request.url);
        const cand_text = cleanText($cand.text());
        if (cand_text.length > description_text.length) {
            description_html = cand_html;
            description_text = cand_text;
        }
      }
    }
  }
  // Fallback 2: Biggest text block
  if (!description_text || description_text.length < 100) {
    const $big = biggestTextBlockHeuristic($);
    if ($big) {
      const big_html = sanitizeDescription($, $big.clone(), request.url);
      const big_text = cleanText($big.text());
       if (big_text.length > description_text.length) {
            description_html = big_html;
            description_text = big_text;
        }
    }
  }

  date_posted = extractDatePosted($) || '';

  // JSON-LD (This is the most reliable source, so it runs last and overwrites)
  const jsonLd = parseJsonLdJob($);
  ({ title, company, job_type, location, salary_range, category, description_html, description_text, date_posted } =
    enrichFromJsonLd(jsonLd, { title, company, job_type, location, salary_range, category, description_html, description_text, date_posted }, request.url));

  return { url: request.url, title, company, job_type, location, salary_range, category, description_html, description_text, date_posted };
};

// ------------------------- MAIN -------------------------
// Wrap the entire run in the Actor.main() lifecycle hook
await Actor.main(async () => {
  // Safely read and validate input
  const input = (await Actor.getInput()) ?? {};
  const {
    keyword, // Use prefilled default from input schema
    location: locationFilter, // Use prefilled default
    posted_date = 'anytime', // Safe default
    results_wanted: RESULTS_WANTED_RAW, // Use prefilled default
    max_pages: MAX_PAGES_RAW, // Use prefilled default
    collectDetails = true, // Safe default
    startUrl,
    url,
    startUrls,
    cookies,
    cookiesJson,
    proxyConfiguration, // Honor the provided proxy config
  } = input;

  // Safely parse numeric inputs with fallbacks
  const RESULTS_WANTED = Number.isFinite(+RESULTS_WANTED_RAW) ? Math.max(1, +RESULTS_WANTED_RAW) : 100; // Use input default
  const MAX_PAGES = Number.isFinite(+MAX_PAGES_RAW) ? Math.max(1, +MAX_PAGES_RAW) : 999; // Use input default

  // Log the sanitized/final input parameters for QA
  log.info('Actor starting with parameters:', {
      keyword,
      locationFilter,
      posted_date,
      RESULTS_WANTED,
      MAX_PAGES,
      collectDetails,
      hasStartUrls: (startUrls && startUrls.length > 0) || !!startUrl || !!url,
      hasProxy: !!proxyConfiguration,
  });

  const initialUrls = [];
  // Build the search URL from inputs
  const builtStartUrl = buildStartUrl(keyword, locationFilter, posted_date);
  
  // Respect user-provided start URLs first
  if (Array.isArray(startUrls) && startUrls.length) {
      initialUrls.push(...startUrls.map(o => (typeof o === 'string' ? o : o?.url)).filter(Boolean));
  }
  if (startUrl && typeof startUrl === 'string') initialUrls.push(startUrl);
  if (url && typeof url === 'string') initialUrls.push(url);
  
  // Fallback to the built search URL if no other URLs were provided
  if (initialUrls.length === 0) {
      initialUrls.push(builtStartUrl);
  }

  // Honor proxy configuration exactly as provided
  const proxyConf = proxyConfiguration ? await Actor.createProxyConfiguration(proxyConfiguration) : undefined;

  let jobsScraped = 0;
  let jobsEnqueued = 0;
  const scrapedUrls = new Set();

  const crawler = new CheerioCrawler({
    proxyConfiguration: proxyConf,
    // Keep performance settings intact as requested
    maxRequestsPerMinute: 120,
    requestHandlerTimeoutSecs: 45,
    navigationTimeoutSecs: 45,
    maxConcurrency: 20, 
    useSessionPool: true,
    persistCookiesPerSession: true,
    sessionPoolOptions: { maxPoolSize: 50, sessionOptions: { maxUsageCount: 50, maxErrorScore: 3 } },
    maxRequestRetries: 5, // Use standard retries
    // Max requests per crawl as a safety rail, respecting limits
    maxRequestsPerCrawl: Math.max(RESULTS_WANTED * (collectDetails ? 3 : 1), MAX_PAGES, 1000),

    preNavigationHooks: [({ request, session }) => {
      // Keep stealth headers intact
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
      // Safely normalize and add cookies
      const cookieHeader = normalizeCookieHeader({ cookies, cookiesJson });
      if (cookieHeader) request.headers.Cookie = cookieHeader;
    }],

    async requestHandler({ request, $, enqueueLinks, response }) {
      // Robust error handling inside the handler
      try {
        if (!$ || typeof $.html !== 'function') {
            log.warning(`Skipping request, Cheerio object not available (HTTP ${response.statusCode}): ${request.url}`);
            request.retry('cheerio-not-loaded'); // Ask for a retry
            return;
        }

        const { label = 'LIST', pageNo = 1 } = request.userData;

        if (label === 'LIST') {
          log.info(`Processing LIST page ${pageNo}: ${request.url}`);
          // collect links & per-card seeds
          const links = collectJobLinks($, request.url);
          if (links.length === 0) {
              log.warning(`No job links found on LIST page: ${request.url}`);
          }
          
          const seedsByUrl = new Map();
          $('a[href*="/listings/"]').each((_, a) => {
            const href = $(a).attr('href');
            const abs = href && toAbs(href, request.url);
            if (!abs) return;
            const u = abs.split('?')[0];
            // Find the closest common ancestor card
            const $card = $(a).closest('li, article, .search-result-item, .job-card, .job-item, .search-result, div[class*="job"], div[class*="listing"]');
            const seed = extractFromListingCard($, $card);
            if (seed.title) seedsByUrl.set(u, seed);
          });

          if (!collectDetails) {
            // Stop enqueueing when limit is hit
            const toPush = links.slice(0, RESULTS_WANTED - jobsScraped);
            if (toPush.length > 0) {
              const items = toPush.map(u => ({ url: u, ...(seedsByUrl.get(u) || {}), _source: 'jobberman.com' }));
              await Dataset.pushData(items);
              jobsScraped += items.length;
              log.info(`Pushed ${items.length} items directly from list page. Total: ${jobsScraped}`);
            }
          } else {
            // Stop enqueueing when limit is hit
            const toEnqueue = links.slice(0, RESULTS_WANTED - jobsEnqueued);
            for (const u of toEnqueue) {
              // Only enqueue if we haven't hit the total scraped/enqueued limit
              if (jobsEnqueued < RESULTS_WANTED && jobsScraped < RESULTS_WANTED) {
                 await enqueueLinks({ urls: [u], userData: { label: 'DETAIL', seed: seedsByUrl.get(u) || {} } });
                 jobsEnqueued++;
              }
            }
            if (toEnqueue.length > 0) log.info(`Enqueued ${toEnqueue.length} detail pages. Total enqueued: ${jobsEnqueued}`);
          }

          // Deterministic stop conditions
          if (jobsScraped >= RESULTS_WANTED || jobsEnqueued >= RESULTS_WANTED) {
              log.info(`Reached 'results_wanted' limit (${RESULTS_WANTED}). Stopping pagination.`);
              return;
          }
          if (pageNo >= MAX_PAGES) {
              log.info(`Reached 'max_pages' limit (${MAX_PAGES}). Stopping pagination.`);
              return;
          }
          if (links.length === 0) {
              log.warning(`No links found on page ${pageNo}, stopping pagination for this branch.`);
              return;
          }

          const nextUrl = findNextUrl($, request.url);
          if (nextUrl) {
            log.info(`Enqueuing next LIST page ${pageNo + 1}: ${nextUrl}`);
            await enqueueLinks({ urls: [nextUrl], userData: { label: 'LIST', pageNo: pageNo + 1 }, forefront: true });
          } else {
            log.info(`No 'next' link found on page ${pageNo}.`);
          }
        }

        if (label === 'DETAIL') {
          // Deterministic stop condition
          if (jobsScraped >= RESULTS_WANTED) {
              log.info(`Skipping detail page, 'results_wanted' limit (${RESULTS_WANTED}) already met.`);
              return;
          }
          if (scrapedUrls.has(request.url)) {
              log.warning(`Skipping duplicate detail URL: ${request.url}`);
              return;
          }
          
          const item = extractFromDetail({ request, $ });
          
          // Validate essential field
          if (!cleanText(item.title)) {
              log.warning(`Skipping detail page with no title: ${request.url}`);
              return; 
          }
          
          await Dataset.pushData(item);
          jobsScraped++;
          scrapedUrls.add(request.url);
          log.info(`Saved: ${item.title} (Total: ${jobsScraped}/${RESULTS_WANTED})`);
        }
      } catch (e) {
        // Log errors gracefully without crashing
        log.error(`Error in requestHandler for ${request.url}: ${e.message}`, { stack: e.stack });
      }
    },

    // Log failed requests clearly
    failedRequestHandler: async ({ request, error }) => {
      log.error(`Request failed: ${request.url} (Label: ${request.userData?.label}, Retries: ${request.retryCount}) | Error: ${error?.message}`);
    },
  });

  log.info('Starting crawler...');
  log.info('--- Initial URLs ---');
  initialUrls.forEach((u, i) => log.info(`${i+1}: ${u}`));
  log.info('----------------------');
  
  // Await the crawler run to ensure all async work completes
  await crawler.run(initialUrls.map(u => ({ url: u, userData: { label: 'LIST', pageNo: 1 } })));
  
  // Final summary log
  if (jobsScraped === 0) {
      log.warning('Crawl finished. No jobs were saved.');
  } else {
      log.info(`Crawl finished. Total jobs saved: ${jobsScraped}`);
  }
});