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

// Keep only text-related tags; strip all attributes **without** using removeAttr (fixes your error)
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

    if (!isTag) return; // ignore text/comment nodes

    // Preserve only absolute href for anchors
    const isAnchor = tag === 'a';
    let href = isAnchor ? (el.attribs?.href || '') : '';

    // Reset attributes safely (avoid $().removeAttr(...))
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
    const isTag = el && el.type === 'tag';
    if (!isTag) return;
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
  const dateMap = { '24h': '1 day', '7d': '7 days', '30d': '30 days' };
  if (date && dateMap[date]) u.searchParams.set('created_at', dateMap[date]);
  return u.href;
};

const normalizeCookieHeader = ({ cookies, cookiesJson }) => {
  if (cookies && typeof cookies === 'string' && cookies.trim()) return cookies.trim();
  if (cookiesJson && typeof cookiesJson === 'string') {
    try {
      const parsed = JSON.parse(cookiesJson);
      if (Array.isArray(parsed)) return parsed.map(c => `${c.name}=${c.value}`).join('; ');
      if (typeof parsed === 'object') return Object.entries(parsed).map(([k,v]) => `${k}=${v}`).join('; ');
    } catch {}
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
    const arr = Array.isArray(parsed) ? parsed : [parsed];
    for (const node of arr) {
      const t = node && node['@type'];
      const isJob = t === 'JobPosting' || (Array.isArray(t) && t.includes('JobPosting'));
      if (isJob) { job = node; return false; }
    }
  });
  return job;
};

const enrichFromJsonLd = (jsonLd, fields, baseUrl) => {
  if (!jsonLd) return fields;
  const out = { ...fields };
  out.title = jsonLd.title || out.title;
  out.company = (jsonLd.hiringOrganization && (jsonLd.hiringOrganization.name || jsonLd.hiringOrganization['@name'])) || out.company;
  out.date_posted = jsonLd.datePosted || out.date_posted;

  if (jsonLd.employmentType)
    out.job_type = Array.isArray(jsonLd.employmentType) ? jsonLd.employmentType.join(', ') : jsonLd.employmentType;

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

  if (jsonLd.industry && !out.category)
    out.category = Array.isArray(jsonLd.industry) ? jsonLd.industry[0] : jsonLd.industry;

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
  $('a[href*="/listings/"]').each((_, a) => {
    const href = $(a).attr('href');
    const abs = href && toAbs(href, base);
    if (abs && !abs.includes('#')) links.add(abs.split('?')[0]);
  });
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
  const m = currentUrl.match(/[?&]page=(\d+)/);
  if (m) return currentUrl.replace(/([?&])page=\d+/, `$1page=${parseInt(m[1], 10) + 1}`);
  return currentUrl.includes('?') ? `${currentUrl}&page=2` : `${currentUrl}?page=2`;
};

// Parse a listing card to seed fields
const extractFromListingCard = ($, $card) => {
  const res = { title: '', company: '', location: '', job_type: '', salary_range: '', category: '' };
  const $titleA = $card.find('a[href*="/listings/"]').first();
  res.title = cleanText($titleA.text());

  const lines = cleanText($card.text()).split(/\s{2,}|\n+/).map(s => s.trim()).filter(Boolean);

  // company
  for (const line of lines) {
    if (line === res.title) continue;
    if (/^(New|Today|Yesterday)$/i.test(line)) continue;
    if (/\b(Easy Apply|FEATURED)\b/i.test(line)) continue;
    if (/\b(NGN|GHS|KES|ZAR|USD)\b\s*\d/i.test(line)) continue;
    if (/\b(Full\s*Time|Part\s*Time|Contract|Temporary|Internship|Remote|Hybrid|Freelance|Volunteer)\b/i.test(line)) continue;
    res.company = line; break;
  }

  // meta: "LOCATION  JOB_TYPE  SALARY"
  const meta = lines.find(l => /\b(Full\s*Time|Part\s*Time|Contract|Temporary|Internship|Remote|Hybrid|Freelance|Volunteer)\b/i.test(l));
  if (meta) {
    const jt = meta.match(/\b(Full\s*Time|Part\s*Time|Contract|Temporary|Internship|Remote|Hybrid|Freelance|Volunteer)\b/i);
    if (jt) res.job_type = jt[1].replace(/\s+/g, ' ');
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
    const candidates = [];
    $('main, article').find('section, div, article').each((_, n) => {
      const $n = $(n);
      const txt = cleanText($n.text() || '');
      if (txt.length > 400) candidates.push({ $n, len: txt.length });
    });
    candidates.sort((a, b) => b.len - a.len);
    if (candidates.length) {
      const $big = candidates[0].$n;
      description_html = sanitizeDescription($, $big.clone(), request.url) || description_html;
      description_text = cleanText($big.text()) || description_text;
    }
  }

  date_posted = extractDatePosted($) || '';

  // JSON-LD
  const jsonLd = parseJsonLdJob($);
  ({ title, company, job_type, location, salary_range, category, description_html, description_text, date_posted } =
    enrichFromJsonLd(jsonLd, { title, company, job_type, location, salary_range, category, description_html, description_text, date_posted }, request.url));

  return { url: request.url, title, company, job_type, location, salary_range, category, description_html, description_text, date_posted };
};

const isDetailPage = ($) =>
  $('article h1, header h1, .job-details h1, h1[class*="job" i]').length > 0 ||
  $('script[type="application/ld+json"]').length > 0;

// ------------------------- MAIN -------------------------
await Actor.init();

const input = (await Actor.getInput()) ?? {};
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

const RESULTS_WANTED = Number.isFinite(+RESULTS_WANTED_RAW) ? Math.max(1, +RESULTS_WANTED_RAW) : Number.MAX_SAFE_INTEGER;
const MAX_PAGES = Number.isFinite(+MAX_PAGES_RAW) ? Math.max(1, +MAX_PAGES_RAW) : 999;

const initialUrls = [];
const builtStartUrl = buildStartUrl(keyword, locationFilter, posted_date);
if (Array.isArray(startUrls) && startUrls.length) initialUrls.push(...startUrls.map(o => o.url || o));
if (startUrl) initialUrls.push(startUrl);
if (url) initialUrls.push(url);
if (initialUrls.length === 0) initialUrls.push(builtStartUrl);

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

  async requestHandler({ request, $, enqueueLinks }) {
    if (!$ || typeof $.html !== 'function') return;

    const { label = 'LIST', pageNo = 1 } = request.userData;

    if (label === 'LIST') {
      // collect links & per-card seeds
      const links = collectJobLinks($, request.url);
      const seedsByUrl = new Map();
      $('a[href*="/listings/"]').each((_, a) => {
        const href = $(a).attr('href');
        const abs = href && toAbs(href, request.url);
        if (!abs) return;
        const u = abs.split('?')[0];
        const $card = $(a).closest('li, article, .search-result-item, .job-card, .job-item, .search-result, div');
        const seed = extractFromListingCard($, $card);
        if (seed.title) seedsByUrl.set(u, seed);
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

      if (jobsScraped >= RESULTS_WANTED || jobsEnqueued >= RESULTS_WANTED || pageNo >= MAX_PAGES) return;

      const nextUrl = findNextUrl($, request.url);
      if (nextUrl) {
        await enqueueLinks({ urls: [nextUrl], userData: { label: 'LIST', pageNo: pageNo + 1 }, forefront: true });
      }
    }

    if (label === 'DETAIL') {
      if (jobsScraped >= RESULTS_WANTED || scrapedUrls.has(request.url)) return;
      try {
        const item = extractFromDetail({ request, $ });
        if (!cleanText(item.title)) return; // essential
        await Dataset.pushData(item);
        jobsScraped++;
        scrapedUrls.add(request.url);
        log.info(`Saved: ${item.title}`);
      } catch (e) {
        log.error(`Error extracting job details from ${request.url}: ${e.message}`);
      }
    }
  },

  failedRequestHandler: async ({ request, error }) => {
    log.error(`Request failed ${request.url}: ${error?.message}`);
  },
});

await crawler.run(initialUrls.map(u => ({ url: u, userData: { label: 'LIST', pageNo: 1 } })));
log.info(`Done. Jobs saved: ${jobsScraped}`);
await Actor.exit();
