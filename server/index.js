const http = require("http");
const fs = require("fs");
const path = require("path");
const {
  getRecentTopicsForSearch,
  getTopicHistory,
  getTrackedSearches,
  saveTopicSnapshots,
  saveTrackedSearch
} = require("./storage");

const rootDir = path.resolve(__dirname, "..");
const indexPath = path.join(rootDir, "index.html");
const port = Number.parseInt(process.env.PORT || "3000", 10);
const OPENAI_TIMEOUT_MS = 1200;
const DISCOVERY_TIMEOUT_MS = 1800;
const RECENT_MONTH_WINDOW = 4;
const SEARCH_CACHE_TTL_MS = 4 * 60 * 1000;

const PLATFORM_FETCHERS = {
  YouTube: fetchYouTube,
  Twitter: fetchTwitter,
  GNews: fetchGNews
};

const searchResponseCache = new Map();
const KNOWN_QUERY_ALIASES = new Map([
  ["ramcharan", ["Ram Charan", "Ram Charan Konidela"]],
  ["alluarjun", ["Allu Arjun"]],
  ["maheshbabu", ["Mahesh Babu"]],
  ["vijaydevarakonda", ["Vijay Deverakonda"]],
  ["rashmikamandanna", ["Rashmika Mandanna"]],
  ["jrntr", ["Jr NTR", "NTR"]],
  ["pawankalyan", ["Pawan Kalyan"]]
]);

function parseCount(value) {
  return Number.parseInt(value || "0", 10);
}

function normaliseDate(value) {
  const date = value ? new Date(value) : new Date();
  return Number.isNaN(date.getTime()) ? new Date().toISOString() : date.toISOString();
}

function parseRelativeDate(value) {
  const text = String(value || "").trim().toLowerCase();
  const match = text.match(/(\d+)\s+(minute|hour|day|week|month|year)s?\s+ago/);
  if (!match) return null;
  const amount = Number.parseInt(match[1], 10);
  const unit = match[2];
  const date = new Date();
  if (unit === "minute") date.setMinutes(date.getMinutes() - amount);
  if (unit === "hour") date.setHours(date.getHours() - amount);
  if (unit === "day") date.setDate(date.getDate() - amount);
  if (unit === "week") date.setDate(date.getDate() - amount * 7);
  if (unit === "month") date.setMonth(date.getMonth() - amount);
  if (unit === "year") date.setFullYear(date.getFullYear() - amount);
  return date.toISOString();
}

function normalizeOptionalDate(value) {
  if (!value) return null;
  const relative = parseRelativeDate(value);
  if (relative) return relative;
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date.toISOString();
}

function extractYouTubeFallbackChannel(source, snippet, title) {
  const sourceText = String(source || "");
  const sourceParts = sourceText.split("·").map(part => part.trim()).filter(Boolean);
  if (sourceParts.length > 1) {
    return sourceParts[sourceParts.length - 1];
  }

  const snippetText = String(snippet || "");
  const snippetParts = snippetText.split("•").map(part => part.trim()).filter(Boolean);
  const likelyChannel = snippetParts.find(part => /[A-Za-z]/.test(part) && !/\bviews?\b/i.test(part) && !/ago$/i.test(part));
  if (likelyChannel) {
    return likelyChannel;
  }

  return sourceText || String(title || "YouTube");
}

function extractYouTubeFallbackTime(item) {
  const direct = normalizeOptionalDate(item.date);
  if (direct) return direct;

  const snippet = String(item.snippet || "");
  const source = String(item.source || "");
  const combined = `${snippet} ${source}`;
  const relativeMatch = combined.match(/(\d+\s+(?:minute|hour|day|week|month|year)s?\s+ago)/i);
  if (relativeMatch) {
    return parseRelativeDate(relativeMatch[1]);
  }

  const premieredMatch = combined.match(/\b(?:premiered|streamed|published)\s+([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})/i);
  if (premieredMatch) {
    return normalizeOptionalDate(premieredMatch[1]);
  }

  const monthDayYearMatch = combined.match(/\b([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})\b/);
  if (monthDayYearMatch) {
    return normalizeOptionalDate(monthDayYearMatch[1]);
  }

  return null;
}

async function fetchYouTubePageMetadata(link) {
  if (!link || !/youtube\.com\/watch/i.test(link)) {
    return null;
  }

  try {
    const response = await fetch(link, {
      headers: {
        "User-Agent": "Mozilla/5.0"
      }
    });
    if (!response.ok) return null;
    const html = await response.text();

    const uploadDateMatch =
      html.match(/"uploadDate":"([^"]+)"/i) ||
      html.match(/itemprop="uploadDate"\s+content="([^"]+)"/i) ||
      html.match(/datePublished":"([^"]+)"/i);

    const channelMatch =
      html.match(/"ownerChannelName":"([^"]+)"/i) ||
      html.match(/"author":"([^"]+)"/i) ||
      html.match(/itemprop="author"[^>]*content="([^"]+)"/i);

    return {
      time: normalizeOptionalDate(uploadDateMatch?.[1]),
      channel: channelMatch?.[1] || null
    };
  } catch {
    return null;
  }
}

function sourceTimeValue(value) {
  if (!value) return Number.POSITIVE_INFINITY;
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? Number.POSITIVE_INFINITY : date.getTime();
}

function sortSourcesChronologically(items = []) {
  return [...items].sort((a, b) => {
    const diff = sourceTimeValue(a.time) - sourceTimeValue(b.time);
    if (diff !== 0) return diff;
    return String(a.channel || "").localeCompare(String(b.channel || ""));
  });
}

function platformScore(platform, metrics) {
  if (platform === "YouTube") {
    const views = Number(metrics.views || 0);
    const likes = Number(metrics.likes || 0);
    const comments = Number(metrics.comments || 0);
    return Math.round(
      Math.log10(views + 10) * 9000 +
      Math.log10(likes + 10) * 12000 +
      Math.log10(comments + 10) * 15000
    );
  }
  if (platform === "Twitter") {
    return Math.round(
      Math.log10(Number(metrics.likes || 0) + 10) * 10000 +
      Math.log10(Number(metrics.retweets || 0) + 10) * 14000 +
      Math.log10(Number(metrics.replies || 0) + 10) * 12000 +
      Math.log10(Number(metrics.quotes || 0) + 10) * 13000
    );
  }
  return metrics.rank * 1000 + metrics.freshness * 5000;
}

function monthsAgoIso(months) {
  const date = new Date();
  date.setMonth(date.getMonth() - months);
  return date.toISOString();
}

function ageInHours(value) {
  const date = new Date(value || Date.now());
  if (Number.isNaN(date.getTime())) return 99999;
  return Math.max(1, (Date.now() - date.getTime()) / 36e5);
}

function freshnessScore(value) {
  const hours = ageInHours(value);
  if (hours <= 24) return 100;
  if (hours <= 72) return 88;
  if (hours <= 24 * 7) return 76;
  if (hours <= 24 * 30) return 60;
  if (hours <= 24 * 90) return 42;
  if (hours <= 24 * 180) return 28;
  if (hours <= 24 * 365) return 16;
  return 6;
}

function freshnessMultiplier(value) {
  return freshnessScore(value) / 100;
}

async function readJson(response, fallbackMessage) {
  let data;
  try {
    data = await response.json();
  } catch {
    throw new Error(fallbackMessage);
  }

  if (!response.ok) {
    const apiMessage =
      data?.error?.message ||
      data?.detail ||
      data?.message ||
      fallbackMessage;
    throw new Error(apiMessage);
  }

  return data;
}

async function fetchJsonWithTimeout(url, options, fallbackMessage, timeoutMs = OPENAI_TIMEOUT_MS) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await readJson(
      await fetch(url, { ...(options || {}), signal: controller.signal }),
      fallbackMessage
    );
  } catch (error) {
    if (error.name === "AbortError") {
      throw new Error(`${fallbackMessage} Timed out.`);
    }
    throw error;
  } finally {
    clearTimeout(timeout);
  }
}

async function createOpenAiJsonResponse({
  systemPrompt,
  userPayload,
  schemaName,
  schema,
  timeoutMs = OPENAI_TIMEOUT_MS,
  useWebSearch = false,
  reasoningEffort = "low"
}) {
  const model = process.env.OPENAI_MODEL || "gpt-5.1";
  const body = {
    model,
    input: [
      {
        role: "system",
        content: [{ type: "input_text", text: systemPrompt }]
      },
      {
        role: "user",
        content: [{ type: "input_text", text: JSON.stringify(userPayload) }]
      }
    ],
    reasoning: { effort: reasoningEffort },
    text: {
      format: {
        type: "json_schema",
        name: schemaName,
        strict: true,
        schema
      }
    }
  };

  if (useWebSearch) {
    body.tools = [{ type: "web_search", external_web_access: true }];
    body.tool_choice = "auto";
  }

  const data = await fetchJsonWithTimeout("https://api.openai.com/v1/responses", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${process.env.OPENAI_API_KEY}`
    },
    body: JSON.stringify(body)
  }, "OpenAI request failed.", timeoutMs);

  return JSON.parse(data.output_text || "{}");
}

async function fetchYouTube(keyword) {
  if (!process.env.YOUTUBE_API_KEY) {
    throw new Error("Missing YOUTUBE_API_KEY in .env");
  }

  async function searchVideos(params) {
    const searchUrl = new URL("https://www.googleapis.com/youtube/v3/search");
    searchUrl.search = new URLSearchParams({
      part: "snippet",
      q: keyword,
      type: "video",
      maxResults: "6",
      key: process.env.YOUTUBE_API_KEY,
      ...params
    }).toString();

    const data = await readJson(
      await fetch(searchUrl),
      "YouTube search request failed."
    );

    return data.items || [];
  }

  const recentItems = await searchVideos({
    order: "date",
    publishedAfter: monthsAgoIso(RECENT_MONTH_WINDOW)
  });
  const relevanceItems = recentItems.length >= 3
    ? []
    : await searchVideos({ order: "relevance", maxResults: "4" });

  const seenVideoIds = new Set();
  const items = [...recentItems, ...relevanceItems].filter(item => {
    const videoId = item.id?.videoId;
    if (!videoId || seenVideoIds.has(videoId)) return false;
    seenVideoIds.add(videoId);
    return true;
  });
  const ids = items.map(item => item.id?.videoId).filter(Boolean);
  if (!ids.length) {
    return [];
  }

  const videosUrl = new URL("https://www.googleapis.com/youtube/v3/videos");
  videosUrl.search = new URLSearchParams({
    part: "statistics",
    id: ids.join(","),
    key: process.env.YOUTUBE_API_KEY
  }).toString();

  const videosData = await readJson(
    await fetch(videosUrl),
    "YouTube video-details request failed."
  );

  const statsMap = new Map((videosData.items || []).map(video => [video.id, video.statistics || {}]));

  return items.map(item => {
    const snippet = item.snippet || {};
    const stats = statsMap.get(item.id.videoId) || {};
    const views = parseCount(stats.viewCount);
    const likes = parseCount(stats.likeCount);
    const comments = parseCount(stats.commentCount);
    const postText = `${snippet.title || ""}. ${snippet.description || ""}`.trim();
    const freshness = freshnessScore(snippet.publishedAt);
    const score =
      Math.round(platformScore("YouTube", { views, likes, comments }) * freshnessMultiplier(snippet.publishedAt)) +
      freshness * 2500;

    return {
      rawTitle: snippet.title || "Untitled video",
      postText,
      time: normaliseDate(snippet.publishedAt),
      channel: snippet.channelTitle || "Unknown channel",
      link: `https://www.youtube.com/watch?v=${item.id.videoId}`,
      platform: "YouTube",
      metrics: { views, likes, comments, freshness },
      metricSummary: `${compact(views)} views | ${compact(likes)} likes | ${compact(comments)} comments | freshness ${freshness}/100`,
      score
    };
  });
}

async function fetchTwitter(keyword) {
  if (!process.env.TWITTER_BEARER_TOKEN) {
    throw new Error("Missing TWITTER_BEARER_TOKEN in .env");
  }

  const url = new URL("https://api.twitter.com/2/tweets/search/recent");
  url.search = new URLSearchParams({
    query: keyword,
    max_results: "8",
    "tweet.fields": "created_at,public_metrics,author_id,text",
    expansions: "author_id",
    "user.fields": "name,username,verified"
  }).toString();

  const data = await readJson(
    await fetch(url, {
      headers: { Authorization: `Bearer ${process.env.TWITTER_BEARER_TOKEN}` }
    }),
    "Twitter search request failed."
  );

  const users = new Map((data.includes?.users || []).map(user => [user.id, user]));

  return (data.data || []).map(tweet => {
    const user = users.get(tweet.author_id) || {};
    const likes = parseCount(tweet.public_metrics?.like_count);
    const retweets = parseCount(tweet.public_metrics?.retweet_count);
    const replies = parseCount(tweet.public_metrics?.reply_count);
    const quotes = parseCount(tweet.public_metrics?.quote_count);

    return {
      rawTitle: shortText(tweet.text || "Untitled tweet", 80),
      postText: tweet.text || "",
      time: normaliseDate(tweet.created_at),
      channel: user.name ? `${user.name}${user.verified ? " (verified)" : ""}` : "Unknown account",
      link: user.username
        ? `https://twitter.com/${user.username}/status/${tweet.id}`
        : `https://twitter.com/i/web/status/${tweet.id}`,
      platform: "Twitter",
      metrics: { likes, retweets, replies, quotes },
      metricSummary: `${compact(likes)} likes | ${compact(retweets)} reposts | ${compact(replies)} replies`,
      score: platformScore("Twitter", { likes, retweets, replies, quotes })
    };
  });
}

async function fetchGNews(keyword) {
  if (!process.env.GNEWS_API_KEY) {
    throw new Error("Missing GNEWS_API_KEY in .env");
  }

  const url = new URL("https://gnews.io/api/v4/search");
  url.search = new URLSearchParams({
    q: keyword,
    lang: "en",
    max: "5",
    sortby: "publishedAt",
    apikey: process.env.GNEWS_API_KEY
  }).toString();

  const data = await readJson(
    await fetch(url),
    "GNews request failed."
  );

  return (data.articles || []).map((article, index) => {
    const hours = Math.max(1, (Date.now() - new Date(article.publishedAt).getTime()) / 36e5);
    const rank = Math.max(1, 10 - index);
    const freshness = Math.max(1, Math.round(24 / hours));

    return {
      rawTitle: article.title || "Untitled article",
      postText: `${article.title || ""}. ${article.description || article.content || ""}`.trim(),
      time: normaliseDate(article.publishedAt),
      channel: article.source?.name || "Unknown publication",
      link: article.url || "#",
      platform: "GNews",
      metrics: { rank, freshness },
      metricSummary: `rank ${rank} | freshness ${freshness}/24`,
      score: platformScore("GNews", { rank, freshness })
    };
  });
}

function compact(value) {
  return new Intl.NumberFormat("en-US", {
    notation: "compact",
    maximumFractionDigits: 1
  }).format(value || 0);
}

function shortText(text, limit = 140) {
  return text.length > limit ? `${text.slice(0, limit).trimEnd()}...` : text;
}

function uniqueStrings(values) {
  return [...new Set((values || []).map(value => String(value || "").trim()).filter(Boolean))];
}

function compactText(value) {
  return String(value || "").toLowerCase().replace(/[^a-z0-9]/g, "");
}

function normalizeTrendQuery(value) {
  return String(value || "")
    .replace(/^[^a-zA-Z0-9]+|[^a-zA-Z0-9]+$/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

const DISCOVERY_STOP_WORDS = new Set([
  "about", "after", "ahead", "amid", "and", "announces", "announcement", "at", "behind",
  "breaks", "celebrates", "date", "for", "from", "gets", "gives", "has", "his", "her",
  "in", "into", "latest", "launch", "movie", "new", "news", "now", "official", "on",
  "posters", "release", "reveals", "says", "shoot", "song", "starts", "teaser", "the",
  "their", "this", "trailer", "update", "video", "with"
]);

const ALLOWED_QUERY_MODIFIERS = new Set([
  "song", "songs", "teaser", "trailer", "glimpse", "release", "movie", "interview",
  "event", "controversy", "review", "look", "poster", "announcement", "launch", "update"
]);
const GENERIC_LINKED_TERMS = new Set([
  "actor", "actress", "indian", "telugu", "official", "posts", "obituary", "today",
  "watch", "latest", "trending", "news", "update", "movie", "song", "teaser", "trailer"
]);

const EXPLORATION_QUERY_TERMS = /\b(latest|news|update|trending|movie|song|teaser|trailer|glimpse|release|interview|event|controversy|launch|model|ultra|one ui|galaxy|review|poster|look)\b/i;

function normalizedNeedles(keyword) {
  return uniqueStrings([keyword, ...tokenize(keyword)]).map(compactText).filter(Boolean);
}

function queryAnchorsKeyword(query, keyword) {
  const compactQuery = compactText(query);
  return normalizedNeedles(keyword).some(needle => needle && compactQuery.includes(needle));
}

function queryAddsUsefulSignal(query, keyword) {
  const domain = inferKeywordDomain(keyword, query);
  const loweredQuery = String(query || "").toLowerCase();
  if (domain === "tech" && /\b(movie|song|teaser|trailer|glimpse|lyrical)\b/i.test(loweredQuery)) {
    return false;
  }
  if (domain === "entertainment" && /\b(one ui|galaxy|smartphone|phone|ultra|chip)\b/i.test(loweredQuery)) {
    return false;
  }

  const keywordTokenSet = tokenize(keyword);
  const extraTokens = String(query || "")
    .toLowerCase()
    .split(/\s+/)
    .filter(Boolean)
    .filter(token => !keywordTokenSet.has(token))
    .filter(token => token.length > 3 && (!DISCOVERY_STOP_WORDS.has(token) || ALLOWED_QUERY_MODIFIERS.has(token)));

  if (compactText(query) === compactText(keyword)) return true;
  return extraTokens.length > 0;
}

function queryPriority(query, domain) {
  const text = String(query || "").toLowerCase();
  let score = 0;
  if (domain === "entertainment") {
    if (/\b(song|video song|lyrical|teaser|trailer|glimpse|movie|release|interview|official)\b/i.test(text)) score += 100;
    if (/\b(latest|trending|update|news)\b/i.test(text)) score += 40;
  } else if (domain === "tech") {
    if (/\b(launch|model|ultra|galaxy|one ui|update|review)\b/i.test(text)) score += 100;
    if (/\b(latest|trending|news)\b/i.test(text)) score += 30;
  } else {
    if (/\b(latest|trending|update|news)\b/i.test(text)) score += 40;
  }
  if (/\s/.test(text)) score += 8;
  return score;
}

function filterAnchoredQueries(keyword, queries, limit = 6) {
  const anchored = uniqueStrings((queries || []).map(normalizeTrendQuery))
    .filter(Boolean)
    .filter(query => queryAnchorsKeyword(query, keyword));
  const useful = anchored.filter(query => queryAddsUsefulSignal(query, keyword));
  return uniqueStrings([keyword, ...useful]).slice(0, limit);
}

function baseEvidenceQueries(keyword, queries, limit = 4) {
  const anchored = filterAnchoredQueries(keyword, queries, 10);
  const domain = inferKeywordDomain(keyword, anchored.join(" | "));
  const semantic = filterAnchoredQueries(keyword, semanticFallbackQueries(keyword, anchored.join(" | ")), 10);
  const aliases = anchored.filter(query => compactText(query) !== compactText(keyword) && !EXPLORATION_QUERY_TERMS.test(query));
  const focused = anchored.filter(query => !EXPLORATION_QUERY_TERMS.test(query) || compactText(query) === compactText(keyword));
  const explorations = anchored.filter(query => EXPLORATION_QUERY_TERMS.test(query) && compactText(query) !== compactText(keyword));

  const preferredExplorations = domain === "tech"
    ? explorations.filter(query => /\b(launch|model|ultra|galaxy|update|review)\b/i.test(query))
    : domain === "entertainment"
      ? explorations.filter(query => /\b(movie|song|teaser|trailer|glimpse|release|interview)\b/i.test(query))
      : explorations;

  if (aliases.length) {
    return uniqueStrings(domain === "entertainment"
      ? [keyword, ...preferredExplorations, ...semantic, ...aliases, ...focused]
      : [keyword, ...aliases, ...preferredExplorations, ...semantic, ...focused]).slice(0, limit);
  }
  return uniqueStrings([keyword, ...preferredExplorations, ...semantic, ...focused]).slice(0, limit);
}

function phraseLooksUseful(phrase, keywordNeedles) {
  const cleaned = normalizeTrendQuery(phrase);
  if (!cleaned) return false;
  const compact = compactText(cleaned);
  if (!compact || compact.length < 5) return false;
  if (keywordNeedles.some(needle => compact.includes(needle) || needle.includes(compact))) {
    return false;
  }

  const tokens = cleaned
    .toLowerCase()
    .split(/\s+/)
    .filter(Boolean);

  if (!tokens.length || tokens.every(token => DISCOVERY_STOP_WORDS.has(token))) {
    return false;
  }

  if (tokens.length === 1 && (tokens[0].length < 5 || DISCOVERY_STOP_WORDS.has(tokens[0]))) {
    return false;
  }

  return true;
}

function candidateQueriesFromText(keyword, text, limit = 6) {
  const keywordNeedles = normalizedNeedles(keyword);
  const scored = new Map();
  const source = String(text || "");
  const titleCaseMatches = source.match(/[A-Z][a-zA-Z0-9]{2,}(?:\s+[A-Z][a-zA-Z0-9]{2,}){0,3}/g) || [];
  const keywordRegex = new RegExp(`(?:${[...tokenize(keyword)].join("|")})`, "i");
  const segments = source
    .split(/[|:;,.!?()\-]/)
    .map(segment => normalizeTrendQuery(segment))
    .filter(Boolean);

  for (const match of [...titleCaseMatches, ...segments]) {
    if (!phraseLooksUseful(match, keywordNeedles)) continue;
    const score = (scored.get(match) || 0) + (keywordRegex.test(match) ? 1 : 2);
    scored.set(match, score);
  }

  return [...scored.entries()]
    .sort((a, b) => b[1] - a[1] || a[0].length - b[0].length)
    .map(([phrase]) => `${keyword} ${phrase}`)
    .slice(0, limit);
}

function discoverAliasesFromText(keyword, text) {
  const compactKeyword = compactText(keyword);
  if (!compactKeyword || /\s/.test(keyword)) return [];

  const matches = String(text || "").match(/[A-Za-z][A-Za-z0-9]+(?:\s+[A-Za-z][A-Za-z0-9]+){1,3}/g) || [];
  return uniqueStrings(
    matches
      .map(match => normalizeTrendQuery(match))
      .filter(Boolean)
      .filter(match => compactText(match) === compactKeyword)
      .filter(match => match.toLowerCase() !== String(keyword).toLowerCase())
  ).slice(0, 4);
}

function needsAliasResolution(keyword) {
  const text = String(keyword || "").trim();
  if (!text || /\s/.test(text)) return false;
  if (!/^[A-Za-z]+$/.test(text)) return false;
  return text.length >= 8;
}

function knownAliasesForKeyword(keyword) {
  const compactKeyword = compactText(keyword);
  return uniqueStrings(KNOWN_QUERY_ALIASES.get(compactKeyword) || []);
}

function liveSourceSummary(platform) {
  if (platform === "YouTube") return "YouTube";
  if (platform === "Twitter") return "X/Twitter";
  if (platform === "GNews") return "GNews";
  return [
    process.env.YOUTUBE_API_KEY ? "YouTube" : null,
    process.env.TWITTER_BEARER_TOKEN ? "X/Twitter" : null,
    process.env.GNEWS_API_KEY ? "GNews" : null
  ].filter(Boolean).join(", ");
}

function extractHeadlineLinkedQueries(keyword, texts) {
  const aliases = discoverAliasesFromText(keyword, texts.join(" | "));
  const compactKeyword = compactText(keyword);
  const seed = aliases[0] || keyword;
  const keywordTokens = tokenize(`${keyword} ${aliases.join(" ")}`);
  const scores = new Map();

  for (const text of texts) {
    const headline = String(text || "");
    const compactHeadline = compactText(headline);
    const isLinked = compactHeadline.includes(compactKeyword) || aliases.some(alias => compactHeadline.includes(compactText(alias)));
    if (!isLinked) continue;

    const matches = headline.match(/[A-Z][a-zA-Z0-9]+/g) || [];
    for (const match of matches) {
      const lower = match.toLowerCase();
      if (keywordTokens.has(lower) || DISCOVERY_STOP_WORDS.has(lower) || GENERIC_LINKED_TERMS.has(lower) || match.length < 4) continue;
      scores.set(match, (scores.get(match) || 0) + 1);
    }
  }

  return [...scores.entries()]
    .sort((a, b) => b[1] - a[1] || a[0].length - b[0].length)
    .map(([term]) => `${seed} ${term}`)
    .slice(0, 5);
}

function inferKeywordDomain(keyword, contextText = "") {
  const source = `${keyword} ${contextText}`.toLowerCase();
  if (/\b(samsung|apple|iphone|pixel|google|xiaomi|oneplus|vivo|oppo|realme|iqoo|nothing|motorola|galaxy|one ui|android|smartphone|phone|laptop|chip|fold|ultra|ios|launch event)\b/i.test(source)) {
    return "tech";
  }
  if (/\b(movie|film|song|teaser|trailer|album|glimpse|ott|box office|director|actor|actress|release date|lyrical)\b/i.test(source)) {
    return "entertainment";
  }
  return "general";
}

function semanticFallbackQueries(keyword, contextText = "") {
  const text = String(keyword || "").trim();
  if (!text) return [];

  const lower = text.toLowerCase();
  const domain = inferKeywordDomain(keyword, contextText);
  const personLike = !/\b(company|brand|phone|smartphone|election|gas|weather|rain|match)\b/i.test(lower);

  const variants = [`${text} latest`, `${text} news`, `${text} update`, `${text} trending`];

  if (domain === "entertainment" || personLike) {
    variants.push(`${text} movie`, `${text} song`, `${text} teaser`, `${text} trailer`, `${text} glimpse`, `${text} release`);
    variants.push(`${text} interview`, `${text} event`, `${text} controversy`);
  }

  if (domain === "tech") {
    variants.push(`${text} launch`, `${text} update news`, `${text} model`, `${text} ultra`, `${text} one ui`, `${text} galaxy`);
  }

  if (personLike && domain !== "tech") {
    variants.push(`${text} latest movie`, `${text} upcoming movie`);
  }

  return uniqueStrings(variants).slice(0, 8);
}

function heuristicQueriesFromItems(keyword, items) {
  const scoredQueries = new Map();

  for (const item of items.slice(0, 14)) {
    const sourceText = [item.rawTitle, shortText(item.postText || "", 220), item.channel]
      .filter(Boolean)
      .join(" | ");

    for (const query of candidateQueriesFromText(keyword, sourceText, 4)) {
      scoredQueries.set(query, (scoredQueries.get(query) || 0) + 1);
    }
  }

  return [...scoredQueries.entries()]
    .sort((a, b) => b[1] - a[1] || a[0].length - b[0].length)
    .map(([query]) => query)
    .slice(0, 4);
}

function stableTopicKey(keyword, item) {
  return compactText(`${keyword}|${item.platform || ""}|${item.rawTitle || item.title || ""}`).slice(0, 180);
}

function getTrustFromScore(score, channel = "") {
  const trustedChannel = /official|news|tv|government|gov|press|times|post|journal|reuters|ap|verified/i.test(channel);
  if (trustedChannel || score > 700000) return "Trusted";
  if (score > 80000) return "Verified";
  return "Unverified";
}

function isTrustedPublisherName(name = "") {
  return /official|verified|news|tv|government|gov|press|times|post|journal|reuters|associated press|ap\b|bbc|cnn|ndtv|eenadu|tv9|n24|india today|the hindu|hindustan times|sony music|t-series|aditya music|saregama|lahari|mythri|geetha arts|dvv|prime video|netflix india|zee music|sun tv|maa tv/i.test(name);
}

function isOfficialEntertainmentPublisher(name = "") {
  return /t-series|sony music|aditya music|saregama|lahari|zee music|mythri|geetha arts|dvv|vriddhi cinemas|sukumar writings|jiostudios|ram charan|t-series telugu|t-series tamil|t-series kannada|t-series malayalam/i.test(name);
}

function classifyTopicContext(text = "") {
  const source = String(text || "").toLowerCase();
  if (/(song|teaser|trailer|lyrical|glimpse|motion poster|movie|film|album|single|audio launch)/i.test(source)) {
    return "entertainment_release";
  }
  if (/(marriage|wedding|date fixed|confirmed|official statement|government|election|accident|policy|announcement)/i.test(source)) {
    return "factual_claim";
  }
  return "general";
}

function itemPlatformStrength(item = {}) {
  if (item.platform === "YouTube") {
    return parseCount(item.metrics?.views) + parseCount(item.metrics?.likes) * 20 + parseCount(item.metrics?.comments) * 30;
  }
  if (item.platform === "Twitter") {
    return parseCount(item.metrics?.likes) * 15 + parseCount(item.metrics?.retweets) * 25 + parseCount(item.metrics?.replies) * 20 + parseCount(item.metrics?.quotes) * 25;
  }
  if (item.platform === "GNews") {
    return parseCount(item.metrics?.rank) * 1000 + parseCount(item.metrics?.freshness) * 5000 + 50000;
  }
  return Number(item.score || 0);
}

function dominantPlatform(items = [], fallbackPlatform = "YouTube") {
  const totals = new Map();
  const counts = new Map();
  for (const item of items) {
    const platform = item.platform || fallbackPlatform;
    totals.set(platform, (totals.get(platform) || 0) + itemPlatformStrength(item));
    counts.set(platform, (counts.get(platform) || 0) + 1);
  }
  let bestPlatform = fallbackPlatform;
  let bestScore = -1;
  for (const [platform, score] of totals.entries()) {
    const combinedScore = score + (counts.get(platform) || 0) * 75000;
    if (combinedScore > bestScore) {
      bestPlatform = platform;
      bestScore = combinedScore;
    }
  }
  return bestPlatform;
}

function buildTrustBreakdown({
  channels = [],
  matchedSources = [],
  representative = {},
  confidence = 55,
  aiFeedback = ""
}) {
  const topicContext = classifyTopicContext(`${representative.rawTitle || ""} ${representative.postText || ""}`);
  const trustedChannelHits = channels.filter(channel => isTrustedPublisherName(channel || ""));
  const newsSources = matchedSources.filter(source => source.platform === "GNews");
  const officialHandleHits = matchedSources.filter(source => /official|verified|gov|press|news|music|films|entertainment/i.test(source.channel || ""));
  const totalViews = matchedSources.reduce((sum, source) => sum + parseCount(source.metrics?.views), 0);
  const totalLikes = matchedSources.reduce((sum, source) => sum + parseCount(source.metrics?.likes), 0);
  const totalComments = matchedSources.reduce((sum, source) => sum + parseCount(source.metrics?.comments), 0);
  const engagementBoost = totalViews > 500000 ? 12 : totalViews > 100000 ? 8 : totalViews > 20000 ? 4 : 0;
  const discussionBoost = totalComments > 500 ? 10 : totalComments > 100 ? 6 : totalComments > 25 ? 3 : 0;
  const likeBoost = totalLikes > 10000 ? 8 : totalLikes > 2000 ? 5 : totalLikes > 300 ? 2 : 0;
  const sourceConsensusBoost = matchedSources.length >= 5 ? 10 : matchedSources.length >= 3 ? 6 : matchedSources.length >= 2 ? 3 : 0;
  const officialNewsHits = newsSources.filter(source => isTrustedPublisherName(source.channel || "")).length;
  const representativeTrusted = isTrustedPublisherName(representative.channel || "");
  const representativeOfficialEntertainment = isOfficialEntertainmentPublisher(representative.channel || "");
  const officialEntertainmentHits = matchedSources.filter(source => isOfficialEntertainmentPublisher(source.channel || "")).length;
  const trustedRepresentativeBoost = representativeTrusted ? 22 : 0;
  const entertainmentBoost = topicContext === "entertainment_release" ? 18 : 0;
  const factualBoost = topicContext === "factual_claim" ? 10 : 0;

  const channelReputationScore = Math.max(
    18,
    Math.min(
      99,
      28 +
      trustedChannelHits.length * 15 +
      trustedRepresentativeBoost +
      officialEntertainmentHits * 8 +
      engagementBoost +
      discussionBoost +
      likeBoost +
      entertainmentBoost +
      (representativeOfficialEntertainment ? 20 : 0) +
      Math.round(Math.min(confidence, 40) * 0.45)
    )
  );
  const officialAccountVerificationScore = Math.max(
    12,
    Math.min(
      98,
      16 +
      officialHandleHits.length * 18 +
      (representativeTrusted ? 24 : 0) +
      (representativeOfficialEntertainment ? 16 : 0) +
      sourceConsensusBoost +
      entertainmentBoost +
      factualBoost +
      Math.round(confidence * 0.22)
    )
  );
  const officialNewsVerificationScore = Math.max(
    15,
    Math.min(
      98,
      18 +
      officialNewsHits * 22 +
      newsSources.length * 10 +
      sourceConsensusBoost +
      (topicContext === "entertainment_release" && representativeTrusted ? 34 : 0) +
      (topicContext === "entertainment_release" && representativeOfficialEntertainment ? 10 : 0) +
      (topicContext === "factual_claim" ? 10 : 0) +
      Math.round(confidence * 0.18)
    )
  );

  let totalTrustScore = Math.round(
    channelReputationScore * 0.36 +
    officialAccountVerificationScore * 0.36 +
    officialNewsVerificationScore * 0.28
  );

  if (topicContext === "entertainment_release" && representativeOfficialEntertainment) {
    totalTrustScore = Math.min(99, Math.max(totalTrustScore, 94));
  }

  return {
    totalTrustScore,
    channelReputationScore,
    officialAccountVerificationScore,
    officialNewsVerificationScore,
    channelReputationLabel: channelReputationScore >= 75 ? "STRONG" : channelReputationScore >= 50 ? "MODERATE" : "WEAK",
    officialAccountVerificationLabel: officialAccountVerificationScore >= 75 ? "MATCHED" : officialAccountVerificationScore >= 50 ? "PARTIAL" : "UNCONFIRMED",
    officialNewsVerificationLabel: officialNewsVerificationScore >= 75 ? "CONFIRMED" : officialNewsVerificationScore >= 50 ? "PARTIAL" : "WEAK",
    aiFeedback
  };
}

function tokenize(text) {
  return new Set(
    String(text || "")
      .toLowerCase()
      .replace(/[^a-z0-9\s]/g, " ")
      .split(/\s+/)
      .filter(token => token.length > 3)
  );
}

function overlapScore(a, b) {
  if (!a.size || !b.size) return 0;
  let common = 0;
  for (const token of a) {
    if (b.has(token)) common += 1;
  }
  return common / Math.max(Math.min(a.size, b.size), 1);
}

function sourceSimilarityToRepresentative(item, representative, topic = null) {
  const repTokens = new Set([
    ...tokenize(representative?.rawTitle),
    ...tokenize(representative?.postText),
    ...tokenize(topic?.title),
    ...tokenize(topic?.summary)
  ]);
  const itemTokens = new Set([
    ...tokenize(item?.rawTitle),
    ...tokenize(item?.postText),
    ...tokenize(item?.channel)
  ]);
  return overlapScore(repTokens, itemTokens);
}

function collectRelatedSources(allItems, topic, matchedSources, representative) {
  const base = new Set([
    ...tokenize(topic.title),
    ...tokenize(topic.summary),
    ...tokenize(topic.why_now),
    ...tokenize(representative?.rawTitle),
    ...tokenize(representative?.postText)
  ]);

  const existingLinks = new Set(matchedSources.map(item => item.link));
  const expanded = [...matchedSources];

  for (const item of allItems) {
    if (!item || existingLinks.has(item.link)) continue;
    const itemTokens = new Set([
      ...tokenize(item.rawTitle),
      ...tokenize(item.postText),
      ...tokenize(item.channel)
    ]);
    const score = overlapScore(base, itemTokens);
    const crossPlatformBonus = matchedSources.some(source => source.platform !== item.platform) ? 0.04 : 0;
    const representativeScore = sourceSimilarityToRepresentative(item, representative, topic);
    if (Math.max(score + crossPlatformBonus, representativeScore) >= 0.34) {
      expanded.push(item);
      existingLinks.add(item.link);
    }
  }

  return expanded;
}

function topicKey(topic) {
  const seed = `${topic.rawTitle || ""} ${topic.aiSummary || ""} ${topic.postText || ""}`;
  return [...tokenize(seed)].sort().slice(0, 10).join("|");
}

function topicFamilyKey(topic) {
  const generic = new Set([
    "official", "video", "lyrics", "lyrical", "song", "teaser", "trailer", "glimpse",
    "interview", "review", "update", "news", "movie", "film", "live", "performance"
  ]);
  const tokens = [
    ...tokenize(topic.rawTitle || topic.title),
    ...tokenize(topic.aiSummary || ""),
    ...tokenize(topic.postText || "")
  ].filter(token => !generic.has(token));
  return [...new Set(tokens)].sort().slice(0, 4).join("|") || topicKey(topic);
}

function finalizeTopicList(topics, hotCount = 4) {
  const ranked = [...topics].sort((a, b) => topicPresentRank(b) - topicPresentRank(a) || b.score - a.score);
  const safeHotCount = ranked.length <= 4
    ? Math.min(2, ranked.length)
    : ranked.length <= 7
      ? Math.min(3, Math.max(2, ranked.length - 2))
      : Math.min(hotCount, Math.max(2, ranked.length - 3));

  const hot = [];
  const related = [];
  const seenFamilies = new Set();

  for (const topic of ranked) {
    const family = topicFamilyKey(topic);
    if (hot.length < safeHotCount && !seenFamilies.has(family)) {
      hot.push({ ...topic, hot: true });
      seenFamilies.add(family);
    } else {
      related.push({ ...topic, hot: false });
    }
  }

  related.sort((a, b) => topicPresentRank(b) - topicPresentRank(a) || b.score - a.score);

  if (!related.length && hot.length > 1) {
    related.push({ ...hot.pop(), hot: false });
  }

  return [...hot, ...related];
}

function mergeTopicLists(primaryTopics, fallbackTopics, hotCount = 4) {
  const merged = [];
  const seen = new Set();

  for (const topic of [...primaryTopics, ...fallbackTopics]) {
    const key = topicKey(topic);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    merged.push({ ...topic, hot: false });
  }

  return finalizeTopicList(merged, hotCount);
}

function topicPresentRank(topic) {
  const freshness = freshnessScore(topic.firstSourceTime || topic.time);
  const sourceCount = Number(topic.metrics?.sources || topic.matchedSources?.length || 1);
  const trust = Number(topic.trustBreakdown?.totalTrustScore || 0);
  const platformCount = new Set(
    (topic.matchedSources || [])
      .map(source => source.platform)
      .filter(Boolean)
  ).size || (topic.platform && topic.platform !== "All" ? 1 : 0);
  const officialBoost = isOfficialEntertainmentPublisher(topic.channel || "") ? 30 : 0;
  const boundedScore = Math.min(Number(topic.score || 0), 2500000);

  return (
    freshness * 50000 +
    sourceCount * 130000 +
    platformCount * 80000 +
    trust * 12000 +
    officialBoost * 15000 +
    boundedScore
  );
}

function buildFallbackAiReport(item) {
  const trust = item.trustBreakdown || {};
  const verdict = Number(trust.totalTrustScore || 0) >= 80
    ? "VERIFIED"
    : Number(trust.totalTrustScore || 0) >= 55
      ? "PARTIALLY VERIFIED"
      : "UNVERIFIED";

  return {
    claim_summary: `This topic centers on ${shortText(item.postText || item.rawTitle || "a trending claim", 180)}.`,
    verification_analysis: `Trust is weighted across channel reputation (${trust.channelReputationScore || 0}%), official account verification (${trust.officialAccountVerificationScore || 0}%), and official news verification (${trust.officialNewsVerificationScore || 0}%).`,
    spread_pattern: `${item.metrics?.sources || item.matchedSources?.length || 1} matched sources are contributing to this topic across ${item.platform || "the tracked"} platform(s).`,
    verdict
  };
}

function enrichItemsWithHistory(items) {
  return items.map(item => {
    const historyTimeline = item.topicKey ? getTopicHistory(item.topicKey) : [];
    return {
      ...item,
      historyTimeline
    };
  });
}

function dedupeItems(items) {
  const seen = new Set();
  return items.filter(item => {
    const key = item.link || `${item.platform}|${item.channel}|${item.rawTitle}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function hydrateStoredTopics(rows, platform) {
  return rows.map(row => {
    const trustScore = Number(row.trust_score || 0);
    const dominantPlatform = row.dominant_platform || platform || "All";
    return {
      title: row.topic_title || "Untitled topic",
      rawTitle: row.topic_title || "Untitled topic",
      postText: row.topic_title || "Stored topic snapshot",
      time: row.published_at || row.recorded_at || null,
      channel: "Stored trend snapshot",
      link: row.source_link || "#",
      platform: dominantPlatform,
      metrics: {
        views: Number(row.views || 0),
        likes: Number(row.likes || 0),
        comments: Number(row.comments || 0),
        sources: Number(row.source_count || 1)
      },
      metricSummary: `${Number(row.source_count || 1)} sources | stored snapshot`,
      score: Number(row.score || 0),
      trust: getTrustFromScore(Number(row.score || 0), ""),
      trustBreakdown: {
        totalTrustScore: trustScore,
        channelReputationScore: trustScore,
        officialAccountVerificationScore: Math.max(20, trustScore - 8),
        officialNewsVerificationScore: Math.max(20, trustScore - 12),
        aiFeedback: "Using the latest stored topic snapshot because live source APIs are temporarily limited."
      },
      aiSummary: row.topic_title || "Stored topic snapshot",
      aiWhyNow: "Returned from the most recent saved search snapshot while live APIs are limited.",
      aiFeedback: "Stored snapshot fallback",
      aiReport: {
        claim_summary: row.topic_title || "Stored topic snapshot",
        verification_analysis: "Live APIs were limited, so this result is taken from the latest stored verified snapshot.",
        spread_pattern: `${Number(row.source_count || 1)} sources were recorded for this topic in the latest saved snapshot.`,
        verdict: trustScore >= 70 ? "VERIFIED" : trustScore >= 50 ? "PARTIALLY VERIFIED" : "UNVERIFIED"
      },
      storedSnapshot: true,
      storedSourceCount: Number(row.source_count || 1),
      firstSourceChannel: "Stored trend snapshot",
      firstSourceTime: row.published_at || row.recorded_at || null,
      firstSourceLink: row.source_link || "#",
      matchedSources: [{
        channel: "Stored trend snapshot",
        time: row.published_at || row.recorded_at || null,
        link: row.source_link || "#",
        platform: dominantPlatform
      }],
      topicKey: row.topic_key,
      hot: false
    };
  });
}

function searchCacheKey(keyword, platform) {
  return `${String(platform || "All").trim()}::${String(keyword || "").trim().toLowerCase()}`;
}

function readCachedSearch(keyword, platform) {
  const key = searchCacheKey(keyword, platform);
  const cached = searchResponseCache.get(key);
  if (!cached) return null;
  if (Date.now() - cached.createdAt > SEARCH_CACHE_TTL_MS) {
    searchResponseCache.delete(key);
    return null;
  }
  return cached.payload;
}

function writeCachedSearch(keyword, platform, payload) {
  searchResponseCache.set(searchCacheKey(keyword, platform), {
    createdAt: Date.now(),
    payload
  });
}

function getEnabledFetchers(platform) {
  if (platform !== "All") {
    return PLATFORM_FETCHERS[platform] ? [PLATFORM_FETCHERS[platform]] : [];
  }

  const enabled = [];
  if (process.env.YOUTUBE_API_KEY) enabled.push(fetchYouTube);
  if (process.env.TWITTER_BEARER_TOKEN) enabled.push(fetchTwitter);
  if (process.env.GNEWS_API_KEY) enabled.push(fetchGNews);
  return enabled;
}

function lowSignalTitle(title = "") {
  const text = String(title || "");
  const hashtags = (text.match(/#/g) || []).length;
  const shortMarkers = /\b(shorts?|shortvideo|fyp|viral\s*video)\b/i.test(text);
  const clickbaitMarkers = /\b(shock|shocking|amazing performance|must watch|unbelievable|revealed|earn|salary|income)\b/i.test(text);
  const mostlyHashtags = hashtags >= 4 && text.replace(/#[^\s]+/g, "").trim().length < 30;
  return mostlyHashtags || (shortMarkers && hashtags >= 2) || clickbaitMarkers;
}

function relevanceWeight(item, keyword, queries) {
  const haystack = `${item.rawTitle || ""} ${item.postText || ""} ${item.channel || ""}`;
  const compactHaystack = compactText(haystack);
  const domain = inferKeywordDomain(keyword, haystack);
  const keywordCompact = compactText(keyword);
  const queryList = uniqueStrings([keyword, ...(queries || [])]);
  const queryHits = queryList.filter(query => compactHaystack.includes(compactText(query))).length;
  const freshness = freshnessScore(item.time);
  const officialChannel = isOfficialEntertainmentPublisher(item.channel || "") || /\bofficial|news|tv|press|reuters|ap\b/i.test(item.channel || "");
  const title = String(item.rawTitle || "");
  const titleLower = title.toLowerCase();
  const entertainmentSignal = /\b(song|video song|lyrical|teaser|trailer|glimpse|release|interview|official|movie|film|poster|look)\b/i.test(title);
  const weakEntertainmentNoise = /\b(shorts?|shortvideo|fyp|viral\s*video|dance|mall|performance|sister|brother|wife|salary|income|business)\b/i.test(titleLower);

  let score = queryHits * 40 + freshness * 3 + (item.platform === "GNews" ? 30 : 0);
  if (compactHaystack.includes(keywordCompact)) score += 50;
  if (officialChannel) score += 45;
  if (domain === "entertainment" && entertainmentSignal) score += 35;
  if (domain === "tech" && /\b(galaxy|ultra|launch|model|review|update)\b/i.test(title)) score += 35;
  if (lowSignalTitle(title)) score -= 90;
  if (domain === "tech" && /\b(dance|wedding|mall|performance)\b/i.test(title)) score -= 80;
  if (domain === "tech" && !/\b(galaxy|ultra|launch|model|review|update|one ui|android|phone|smartphone|fold|flip|tablet|chip|s\d{2})\b/i.test(title)) score -= 70;
  if (domain === "entertainment" && weakEntertainmentNoise && !entertainmentSignal) score -= 85;
  if (domain === "entertainment" && /\b(song|video song|lyrical|teaser|trailer|glimpse|official update|release date|movie)\b/i.test(title)) score += 40;
  if (domain === "entertainment" && /#/.test(title) && !/\b(song|movie|teaser|trailer|glimpse|official)\b/i.test(titleLower)) score -= 65;
  if (domain === "entertainment" && !entertainmentSignal && !officialChannel && item.platform === "YouTube") score -= 95;
  return score;
}

function filterRelevantItems(items, keyword, queries) {
  const normalizedNeedles = uniqueStrings([keyword, ...(queries || [])])
    .map(compactText)
    .filter(Boolean);
  const keywordTokens = tokenize(keyword);
  const keywordCompact = compactText(keyword);
  const singleTokenKeyword = keywordTokens.size <= 1;
  const linkedTokens = new Set(
    uniqueStrings(queries)
      .flatMap(query => String(query || "").toLowerCase().split(/\s+/))
      .filter(token => token.length > 3)
      .filter(token => !keywordTokens.has(token))
      .filter(token => !DISCOVERY_STOP_WORDS.has(token))
      .filter(token => !GENERIC_LINKED_TERMS.has(token))
  );

  const domain = inferKeywordDomain(keyword, uniqueStrings(queries).join(" "));
  const filtered = items.filter(item => {
    const haystack = compactText(`${item.rawTitle || ""} ${item.postText || ""} ${item.channel || ""}`);
    const freshEnough = freshnessScore(item.time) >= 12 || item.platform === "GNews";
    const mentionsKeywordDirectly = keywordCompact && haystack.includes(keywordCompact);
    const itemTokens = new Set([
      ...tokenize(item.rawTitle),
      ...tokenize(item.postText),
      ...tokenize(item.channel)
    ]);
    const linkedTokenHits = [...linkedTokens].filter(token => itemTokens.has(token)).length;
    const mentionsLinkedToken = linkedTokenHits > 0;
    if (normalizedNeedles.some(needle => needle && haystack.includes(needle))) {
      return freshEnough && (mentionsKeywordDirectly || !singleTokenKeyword);
    }

    if (singleTokenKeyword) {
      return freshEnough && (mentionsKeywordDirectly || linkedTokenHits >= 2 || mentionsLinkedToken);
    }

    return freshEnough && (mentionsKeywordDirectly || linkedTokenHits >= 2 || mentionsLinkedToken || overlapScore(keywordTokens, itemTokens) >= 0.28);
  })
  .map(item => ({ item, weight: relevanceWeight(item, keyword, queries) }))
  .filter(entry => entry.weight > -10)
  .sort((a, b) => b.weight - a.weight || (b.item.score || 0) - (a.item.score || 0))
  .map(entry => entry.item);

  if (filtered.length) {
    const ranked = filtered;
    if (domain === "entertainment") {
      const preferred = ranked.filter(item => {
        const title = String(item.rawTitle || "").toLowerCase();
        const official = isOfficialEntertainmentPublisher(item.channel || "") || /\bofficial|music|films|entertainment\b/i.test(item.channel || "");
        const signal = /\b(song|video song|lyrical|teaser|trailer|glimpse|release|interview|official|movie|film|poster|look)\b/i.test(title);
        return item.platform === "GNews" || official || signal;
      });
      if (preferred.length) {
        return preferred;
      }
    }
    return ranked;
  }

  return items.filter(item => {
    const haystack = compactText(`${item.rawTitle || ""} ${item.postText || ""} ${item.channel || ""}`);
    return keywordCompact && haystack.includes(keywordCompact);
  })
  .map(item => ({ item, weight: relevanceWeight(item, keyword, queries) }))
  .sort((a, b) => b.weight - a.weight || (b.item.score || 0) - (a.item.score || 0))
  .map(entry => entry.item);
}

async function fetchSerpApiTrendQueries(keyword) {
  if (!process.env.SERPAPI_KEY) {
    return [];
  }

  const url = new URL("https://serpapi.com/search.json");
  url.search = new URLSearchParams({
    engine: "google_trends",
    q: keyword,
    data_type: "RELATED_QUERIES",
    date: "now 7-d",
    api_key: process.env.SERPAPI_KEY
  }).toString();

  const data = await fetchJsonWithTimeout(
    url,
    {},
    "SerpApi Google Trends request failed.",
    2200
  );

  const containers = [
    ...(Array.isArray(data.related_queries?.rising) ? data.related_queries.rising : []),
    ...(Array.isArray(data.related_queries?.top) ? data.related_queries.top : []),
    ...(Array.isArray(data.rising) ? data.rising : []),
    ...(Array.isArray(data.top) ? data.top : [])
  ];

  return uniqueStrings(
    containers
      .map(item => normalizeTrendQuery(item.query || item.value || item.term || ""))
      .filter(Boolean)
      .filter(query => query.toLowerCase() !== keyword.toLowerCase())
  ).slice(0, 5);
}

async function fetchSerpApiNewsHeadlines(keyword) {
  if (!process.env.SERPAPI_KEY) {
    return [];
  }

  const url = new URL("https://serpapi.com/search.json");
  url.search = new URLSearchParams({
    engine: "google_news",
    q: keyword,
    api_key: process.env.SERPAPI_KEY
  }).toString();

  const data = await fetchJsonWithTimeout(
    url,
    {},
    "SerpApi Google News request failed.",
    2200
  );

  return (Array.isArray(data.news_results) ? data.news_results : [])
    .map(item => String(item.title || "").trim())
    .filter(Boolean)
    .slice(0, 8);
}

async function fetchSerpApiSearchHeadlines(keyword) {
  if (!process.env.SERPAPI_KEY) {
    return [];
  }

  const url = new URL("https://serpapi.com/search.json");
  url.search = new URLSearchParams({
    engine: "google",
    q: keyword,
    api_key: process.env.SERPAPI_KEY
  }).toString();

  const data = await fetchJsonWithTimeout(
    url,
    {},
    "SerpApi Google Search request failed.",
    2200
  );

  return (Array.isArray(data.organic_results) ? data.organic_results : [])
    .flatMap(item => [String(item.title || "").trim(), String(item.snippet || "").trim()])
    .filter(Boolean)
    .slice(0, 8);
}

async function fetchSerpApiYouTubeFallback(keyword) {
  if (!process.env.SERPAPI_KEY) {
    return [];
  }

  const url = new URL("https://serpapi.com/search.json");
  url.search = new URLSearchParams({
    engine: "google",
    q: `site:youtube.com/watch ${keyword}`,
    api_key: process.env.SERPAPI_KEY
  }).toString();

  const data = await fetchJsonWithTimeout(
    url,
    {},
    "SerpApi YouTube fallback request failed.",
    8000
  );

  const candidates = (Array.isArray(data.organic_results) ? data.organic_results : [])
    .filter(item => /youtube\.com\/watch/i.test(String(item.link || "")))
    .slice(0, 8);

  const enriched = await Promise.all(
    candidates.map(async (item, index) => {
      let fallbackTime = extractYouTubeFallbackTime(item);
      let channel = extractYouTubeFallbackChannel(item.source, item.snippet, item.title);

      if (!fallbackTime || !channel || /^YouTube$/i.test(channel)) {
        const pageMeta = await fetchYouTubePageMetadata(item.link || "");
        fallbackTime = fallbackTime || pageMeta?.time || null;
        channel = channel && !/^YouTube$/i.test(channel) ? channel : (pageMeta?.channel || channel);
      }

      const hours = Math.max(1, (Date.now() - new Date(fallbackTime || Date.now()).getTime()) / 36e5);
      const freshness = Math.max(16, Math.round(100 / Math.max(1, hours / 24)));
      const rank = Math.max(1, 10 - index);
      return {
        rawTitle: item.title || "Untitled video",
        postText: `${item.title || ""}. ${item.snippet || ""}`.trim(),
        time: fallbackTime,
        channel,
        link: item.link || "#",
        platform: "YouTube",
        metrics: { views: 0, likes: 0, comments: 0, freshness, rank },
        metricSummary: `google rank ${rank} | freshness ${freshness}/100`,
        score: rank * 9000 + freshness * 7000
      };
    })
  );

  return enriched;
}

function extractDiscoveryQueries(keyword, trendQueries, newsHeadlines, searchHeadlines) {
  const combinedHeadlines = [...(newsHeadlines || []), ...(searchHeadlines || [])];
  const headlineText = combinedHeadlines.join(" | ");
  const headlineQueries = candidateQueriesFromText(keyword, headlineText, 8);
  const linkedHeadlineQueries = extractHeadlineLinkedQueries(keyword, combinedHeadlines);
  const semanticQueries = semanticFallbackQueries(keyword, headlineText);
  const trendExpanded = (trendQueries || []).flatMap(query => {
    const cleaned = normalizeTrendQuery(query);
    if (!cleaned) return [];
    return keywordNeedlesMatch(keyword, cleaned) ? [] : [`${keyword} ${cleaned}`];
  });

  return filterAnchoredQueries(keyword, [...linkedHeadlineQueries, ...semanticQueries, ...headlineQueries, ...trendExpanded], 10);
}

function keywordNeedlesMatch(keyword, value) {
  const compactValue = compactText(value);
  return normalizedNeedles(keyword).some(needle => needle && compactValue.includes(needle));
}

async function expandSearchQueries(keyword, platform) {
  const heuristicQueries = semanticFallbackQueries(keyword, "");
  const detectedAliases = uniqueStrings([
    ...knownAliasesForKeyword(keyword),
    ...(needsAliasResolution(keyword)
      ? [
          keyword.replace(/([a-z])([A-Z])/g, "$1 $2"),
          keyword.replace(/([a-z])([A-Z0-9])/g, "$1 $2")
        ]
      : [])
  ]).filter(alias => alias && alias.toLowerCase() !== keyword.toLowerCase());
  const fallbackQueries = filterAnchoredQueries(keyword, [keyword, ...detectedAliases, ...heuristicQueries], 4);
  const shouldForceAliasDiscovery = needsAliasResolution(keyword) && !detectedAliases.length;
  const sourceSummary = liveSourceSummary(platform);
  if (!process.env.OPENAI_API_KEY || platform !== "All") {
    return fallbackQueries;
  }

  try {
    const parsed = await createOpenAiJsonResponse({
      systemPrompt: "You are an AI search planner for a viral trend dashboard. The user gives a keyword, and you must infer what related viral videos, posts, or news developments are most likely trending right now around that keyword. Do not just repeat the exact keyword. If the keyword is a person, film, brand, or product, resolve likely aliases first and then suggest 3 to 5 concrete discovery queries that can surface related viral coverage across the available live sources. Prefer songs, teasers, trailers, launches, updates, interviews, controversy hooks, event/location hooks, official reveals, and breaking developments that are clearly connected to the keyword.",
      userPayload: { keyword, platform, sourceSummary, detectedAliases, heuristicQueries, today: new Date().toISOString().slice(0, 10) },
      schemaName: "search_plan",
      schema: {
        type: "object",
        additionalProperties: false,
        properties: {
          aliases: {
            type: "array",
            items: { type: "string" }
          },
          queries: {
            type: "array",
            items: { type: "string" }
          }
        },
        required: ["aliases", "queries"]
      },
      timeoutMs: DISCOVERY_TIMEOUT_MS,
      useWebSearch: false,
      reasoningEffort: "medium"
    });
    return filterAnchoredQueries(
      keyword,
      [
        keyword,
        ...detectedAliases,
        ...(parsed.aliases || []),
        ...heuristicQueries,
        ...(parsed.queries || [])
      ],
      5
    );
  } catch {
    return fallbackQueries;
  }
}

async function refineQueriesFromResults(keyword, platform, items) {
  if (!process.env.OPENAI_API_KEY || platform !== "All" || !items.length) {
    return heuristicQueriesFromItems(keyword, items);
  }

  try {
    const sourceItems = items.slice(0, 8).map(item => ({
      title: item.rawTitle,
      text: shortText(item.postText || "", 220),
      platform: item.platform,
      channel: item.channel,
      time: item.time
    }));

    const parsed = await createOpenAiJsonResponse({
      systemPrompt: "You are a trend refiner. Read the first-wave live results for a keyword and identify 2 to 4 highly specific emerging subtopics that should be searched next. Only return subtopics clearly grounded in the result text. Prefer current hooks such as product model names, launches, leaks, teaser names, release names, interview angles, location-specific developments, and breaking updates.",
      userPayload: { keyword, sourceItems, today: new Date().toISOString().slice(0, 10) },
      schemaName: "refined_queries",
      schema: {
        type: "object",
        additionalProperties: false,
        properties: {
          queries: {
            type: "array",
            items: { type: "string" }
          }
        },
        required: ["queries"]
      },
      timeoutMs: 1100,
      useWebSearch: false,
      reasoningEffort: "low"
    });
    return filterAnchoredQueries(keyword, [
      keyword,
      ...(parsed.queries || []).map(normalizeTrendQuery),
      ...heuristicQueriesFromItems(keyword, items)
    ], 4);
  } catch {
    return heuristicQueriesFromItems(keyword, items);
  }
}

function buildMergedTopics(keyword, platform, items) {
  const groups = [];
  const sortedItems = [...items].sort((a, b) => (b.score || 0) - (a.score || 0));

  for (const item of sortedItems) {
    const itemTokens = new Set([
      ...tokenize(item.rawTitle),
      ...tokenize(item.postText),
      ...tokenize(item.channel),
      ...tokenize(keyword)
    ]);

    let bestGroup = null;
    let bestScore = 0;

    for (const group of groups) {
      const score = overlapScore(group.tokens, itemTokens);
      if (score > bestScore) {
        bestScore = score;
        bestGroup = group;
      }
    }

    if (bestGroup && bestScore >= 0.24) {
      bestGroup.items.push(item);
      bestGroup.tokens = new Set([...bestGroup.tokens, ...itemTokens]);
    } else {
      groups.push({ items: [item], tokens: itemTokens });
    }
  }

  return groups
    .map(group => {
      const matchedSources = sortSourcesChronologically(group.items);
      const representative = [...group.items].sort((a, b) => (b.score || 0) - (a.score || 0))[0];
      const closeMatches = matchedSources.filter(item => sourceSimilarityToRepresentative(item, representative) >= 0.46);
      const firstSourcePool = closeMatches.length ? closeMatches : matchedSources;
      const firstSource = firstSourcePool.find(item => item.time) || firstSourcePool[0] || representative;
      const dominant = dominantPlatform(group.items, representative.platform);
      const platformDiversity = new Set(group.items.map(item => item.platform).filter(Boolean)).size;
      const totalViews = group.items.reduce((sum, item) => sum + parseCount(item.metrics?.views), 0);
      const totalLikes = group.items.reduce((sum, item) => sum + parseCount(item.metrics?.likes), 0);
      const totalComments = group.items.reduce((sum, item) => sum + parseCount(item.metrics?.comments), 0);
      const freshItems = group.items.reduce((sum, item) => sum + freshnessScore(item.time), 0);
      const score =
        Math.min(group.items.reduce((sum, item) => sum + (item.score || 0), 0), 2500000) +
        freshItems * 50000 +
        group.items.length * 180000 +
        platformDiversity * 180000 +
        freshnessScore(firstSource.time || representative.time) * 120000;

      return {
        title: representative.rawTitle || `${keyword}: Emerging topic`,
        rawTitle: representative.rawTitle || `${keyword}: Emerging topic`,
        postText: shortText(representative.postText || representative.rawTitle || "", 500),
        time: representative.time,
        channel: representative.channel,
        link: representative.link,
        platform: platform === "All" ? dominant : representative.platform,
        metrics: {
          views: totalViews,
          likes: totalLikes,
          comments: totalComments,
          sources: matchedSources.length
        },
        metricSummary: `${matchedSources.length} sources | ${platformDiversity} platforms | freshness ${Math.round(freshItems / Math.max(1, group.items.length))}/100`,
        score,
        trust: getTrustFromScore(score, representative.channel || ""),
        trustBreakdown: buildTrustBreakdown({
          channels: group.items.map(item => item.channel),
          matchedSources,
          representative,
          confidence: Math.min(85, 40 + matchedSources.length * 8 + platformDiversity * 6),
          aiFeedback: `${matchedSources.length} sources mention this topic. Trust is weighted by source reputation, likely official-account alignment, and news verification strength.`
        }),
        aiSummary: shortText(representative.postText || representative.rawTitle || "", 180),
        aiWhyNow: `${matchedSources.length} matching sources are posting about this topic right now.`,
        aiReport: buildFallbackAiReport({
          postText: representative.postText || representative.rawTitle || "",
          rawTitle: representative.rawTitle,
          platform: dominant,
          metrics: { sources: matchedSources.length },
          matchedSources,
          trustBreakdown: buildTrustBreakdown({
            channels: group.items.map(item => item.channel),
            matchedSources,
            representative,
            confidence: Math.min(85, 40 + matchedSources.length * 8 + platformDiversity * 6),
            aiFeedback: `${matchedSources.length} sources mention this topic. Trust is weighted by source reputation, likely official-account alignment, and news verification strength.`
          })
        }),
        firstSourceChannel: firstSource.channel,
        firstSourceTime: firstSource.time || representative.time || null,
        firstSourceLink: firstSource.link,
        matchedSources,
        topicKey: stableTopicKey(keyword, { rawTitle: representative.rawTitle || `${keyword}: Emerging topic`, platform: dominant }),
        hot: false
      };
    })
    .sort((a, b) => b.score - a.score);
}

async function analyzeTrendTopics(keyword, platform, items) {
  if (!process.env.OPENAI_API_KEY || !items.length) {
    return null;
  }

  const rankedItems = [...items]
    .map(item => ({ item, weight: relevanceWeight(item, keyword, items.map(entry => entry.rawTitle).slice(0, 6)) }))
    .sort((a, b) => b.weight - a.weight || (b.item.score || 0) - (a.item.score || 0))
    .map(entry => entry.item);

  const sourceItems = rankedItems.slice(0, 10).map(item => ({
    index: items.indexOf(item),
    platform: item.platform,
    channel: item.channel,
    time: item.time,
    rawTitle: item.rawTitle,
    postText: shortText(item.postText || "", 500),
    metricSummary: item.metricSummary,
    score: item.score
  }));

  const parsed = await createOpenAiJsonResponse({
    systemPrompt: "You are a viral trend analyst. First study the fetched evidence from the available live sources and infer what subtopics are actually going viral around the keyword right now. Then group similar source items into current trend topics. Rank topics by present momentum using freshness, repetition across sources, engagement, source credibility, and cross-source convergence. Prefer what is trending now, not just what has total lifetime views. Old high-view videos or old viral posts should be down-ranked unless there is fresh multi-source evidence that they are resurging now. For people, brands, products, and films, prioritize the newest launches, leaks, official reveals, release chatter, songs, teasers, interviews, controversies, and breaking discussion over legacy evergreen content. Prefer topics backed by multiple sources when available, but do not ignore strongly viral official-source topics from a single platform. Keep nearby but meaningfully different developments separate. Generate clear AI topic titles from the evidence instead of copying one raw source title. For each topic, score trust using exactly three factors: 1) channel/account reputation, 2) likely verification against official accounts/handles mentioned in the claim, 3) official news verification. If an entertainment release or song is posted by a clearly official music/studio/distributor account, assign very high trust. Also generate a topic-specific intelligence report with claim summary, verification analysis, spread pattern, verdict, and one short AI feedback sentence. Return only JSON matching the schema.",
    userPayload: { keyword, platform, sourceSummary: liveSourceSummary(platform), items: sourceItems, today: new Date().toISOString().slice(0, 10) },
    schemaName: "trend_topics",
    schema: {
      type: "object",
      additionalProperties: false,
      properties: {
        topics: {
          type: "array",
          items: {
            type: "object",
            additionalProperties: false,
            properties: {
              title: { type: "string" },
              summary: { type: "string" },
              why_now: { type: "string" },
              ai_feedback: { type: "string" },
              claim_summary: { type: "string" },
              verification_analysis: { type: "string" },
              spread_pattern: { type: "string" },
              verdict: { type: "string" },
              momentum_score: { type: "integer", minimum: 1, maximum: 100 },
              confidence: { type: "integer", minimum: 1, maximum: 100 },
              channel_reputation_score: { type: "integer", minimum: 1, maximum: 100 },
              official_account_verification_score: { type: "integer", minimum: 1, maximum: 100 },
              official_news_verification_score: { type: "integer", minimum: 1, maximum: 100 },
              source_indexes: {
                type: "array",
                items: { type: "integer", minimum: 0 }
              }
            },
            required: ["title", "summary", "why_now", "ai_feedback", "claim_summary", "verification_analysis", "spread_pattern", "verdict", "momentum_score", "confidence", "channel_reputation_score", "official_account_verification_score", "official_news_verification_score", "source_indexes"]
          }
        }
      },
      required: ["topics"]
    },
    timeoutMs: 1800,
    useWebSearch: false,
    reasoningEffort: "medium"
  });
  const topics = Array.isArray(parsed.topics) ? parsed.topics : [];
  if (!topics.length) {
    return null;
  }

  return topics.map((topic, idx) => {
    let matchedSources = (topic.source_indexes || [])
      .map(sourceIndex => items[sourceIndex])
      .filter(Boolean);

    const representative = matchedSources[0] || items[idx] || items[0];
    const topicTitle = String(topic.title || representative?.rawTitle || `${keyword}: Emerging topic`).trim();
    if (platform === "All") {
      matchedSources = collectRelatedSources(items, topic, matchedSources, representative);
    }
    const sortedMatchedSources = sortSourcesChronologically(matchedSources);
    const closeMatches = sortedMatchedSources.filter(item => sourceSimilarityToRepresentative(item, representative, topic) >= 0.46);
    const firstSourcePool = closeMatches.length ? closeMatches : sortedMatchedSources;
    const firstSource = firstSourcePool.find(item => item.time) || firstSourcePool[0] || representative;
    const orderedSources = [...sortedMatchedSources]
      .map(item => ({
        channel: item.channel,
        time: item.time,
        link: item.link,
        platform: item.platform
      }));
    const totalViews = matchedSources.reduce((sum, item) => sum + parseCount(item.metrics?.views), 0);
    const totalLikes = matchedSources.reduce((sum, item) => sum + parseCount(item.metrics?.likes), 0);
    const totalComments = matchedSources.reduce((sum, item) => sum + parseCount(item.metrics?.comments), 0);
    const mergedChannels = [...new Set(matchedSources.map(item => item.channel).filter(Boolean))];
    const dominant = dominantPlatform(matchedSources, representative.platform);
    const platformDiversity = new Set(matchedSources.map(item => item.platform).filter(Boolean)).size;
    const freshnessAverage = Math.round(
      matchedSources.reduce((sum, item) => sum + freshnessScore(item.time), 0) / Math.max(1, matchedSources.length)
    );
    const score =
      topic.momentum_score * 28000 +
      Math.min(matchedSources.reduce((sum, item) => sum + (item.score || 0), 0), 2500000) +
      freshnessAverage * 65000 +
      matchedSources.length * 150000 +
      platformDiversity * 180000 +
      freshnessScore(firstSource.time || representative.time) * 120000;

    return {
      title: topicTitle,
      rawTitle: topicTitle,
      postText: `${topic.summary} ${topic.why_now}`.trim(),
      time: representative.time,
      channel: mergedChannels[0] || representative.channel,
      link: representative.link,
        platform: platform === "All" ? dominant : platform,
        metrics: {
          views: totalViews,
          likes: totalLikes,
          comments: totalComments,
          sources: matchedSources.length
      },
      metricSummary: `${matchedSources.length} sources | ${platformDiversity} platforms | momentum ${topic.momentum_score}/100 | freshness ${freshnessAverage}/100 | confidence ${topic.confidence}/100`,
      score,
      trust: getTrustFromScore(score, mergedChannels[0] || ""),
      trustBreakdown: buildTrustBreakdown({
        channels: mergedChannels,
        matchedSources,
        representative,
        confidence: topic.confidence,
        aiFeedback: topic.ai_feedback
      }),
      aiSummary: topic.summary,
      aiWhyNow: topic.why_now,
      aiFeedback: topic.ai_feedback,
      aiReport: {
        claim_summary: topic.claim_summary,
        verification_analysis: topic.verification_analysis,
        spread_pattern: topic.spread_pattern,
        verdict: topic.verdict
      },
      firstSourceChannel: firstSource.channel,
      firstSourceTime: firstSource.time || representative.time || null,
      firstSourceLink: firstSource.link,
      matchedSources: orderedSources,
      topicKey: stableTopicKey(keyword, { rawTitle: topicTitle, platform: dominant }),
      hot: false
    };
    });
  }

async function fetchByPlatform(keyword, platform, queries = [keyword]) {
  const searchQueries = uniqueStrings(queries).slice(0, platform === "All" ? 4 : 2);

  if (platform !== "All") {
    const fetcher = PLATFORM_FETCHERS[platform];
    if (!fetcher) throw new Error("Unsupported platform selected.");
    const settled = await Promise.allSettled(searchQueries.map(query => fetcher(query)));
    const items = [];
    const errors = [];
    let successCount = 0;
    settled.forEach(result => {
      if (result.status === "fulfilled") {
        successCount += 1;
        items.push(...result.value);
      }
      else errors.push(result.reason?.message || "Unknown platform error");
    });
    if (!items.length && errors.length && !successCount) {
      throw new Error(errors.join(" | "));
    }
    return dedupeItems(items);
  }

  const enabledFetchers = getEnabledFetchers(platform);
  if (!enabledFetchers.length) {
    throw new Error("No configured platforms are available.");
  }
  const items = [];
  const errors = [];
  const domain = inferKeywordDomain(keyword, searchQueries.join(" "));
  const prioritizedQueries = [...searchQueries].sort((a, b) => queryPriority(b, domain) - queryPriority(a, domain));

  const youtubeQueryLimit = domain === "entertainment" ? 3 : domain === "tech" ? 2 : 2;
  const newsQueryLimit = domain === "entertainment" ? 2 : 1;
  const youtubeQueries = uniqueStrings(prioritizedQueries).slice(0, youtubeQueryLimit);
  const newsQueries = uniqueStrings(prioritizedQueries).slice(0, newsQueryLimit);
  const otherQueries = prioritizedQueries.slice(0, 1);

  for (const fetcher of enabledFetchers) {
    const sourceName = fetcher === fetchYouTube ? "YouTube" : fetcher === fetchGNews ? "GNews" : "Twitter";
    const sourceQueries = fetcher === fetchYouTube ? youtubeQueries : fetcher === fetchGNews ? newsQueries : otherQueries;
    let sourceSucceeded = false;

    for (const query of sourceQueries) {
      try {
        const result = await fetcher(query);
        if (Array.isArray(result) && result.length) {
          items.push(...result);
          sourceSucceeded = true;
        }
      } catch (error) {
        errors.push(`${sourceName}: ${error.message || "Unknown platform error"}`);
      }
    }

    if (sourceSucceeded && fetcher === fetchYouTube && items.length >= 4) {
      break;
    }
  }

  if (!items.length && errors.length) {
    throw new Error(errors.join(" | "));
  }

  return dedupeItems(items);
}

async function ensureYouTubePresence(platform, queries, items) {
  return items;
}

function sendJson(res, statusCode, payload) {
  res.writeHead(statusCode, { "Content-Type": "application/json; charset=utf-8" });
  res.end(JSON.stringify(payload));
}

function simplifyApiErrorMessage(message) {
  const text = String(message || "");
  const parts = [];
  if (/quota/i.test(text) || /youtube/i.test(text)) {
    parts.push("YouTube API quota is currently exhausted.");
  }
  if (/rate.?limit/i.test(text) || /too many requests/i.test(text) || /developer accounts are limited/i.test(text) || /news api/i.test(text) || /gnews/i.test(text)) {
    parts.push("GNews is currently rate-limited.");
  }
  if (!parts.length) {
    return "Live source APIs are temporarily unavailable.";
  }
  return `${parts.join(" ")} Showing stored topics when available.`;
}

const server = http.createServer(async (req, res) => {
  const requestUrl = new URL(req.url, `http://${req.headers.host}`);

  if (req.method === "GET" && requestUrl.pathname === "/api/search") {
    const keyword = (requestUrl.searchParams.get("keyword") || "").trim();
    const platform = (requestUrl.searchParams.get("platform") || "All").trim();

    if (!keyword) {
      sendJson(res, 400, { error: "Missing keyword" });
      return;
    }

    try {
      const cached = readCachedSearch(keyword, platform);
      if (cached) {
        sendJson(res, 200, cached);
        return;
      }

      let plannedQueries = [keyword];
      try {
        plannedQueries = await expandSearchQueries(keyword, platform);
      } catch (error) {
        console.warn("OpenAI search planning skipped:", error.message);
      }

      let searchQueries = baseEvidenceQueries(keyword, plannedQueries, 5);
      let fetchedItems = await fetchByPlatform(keyword, platform, searchQueries);
      fetchedItems = await ensureYouTubePresence(platform, searchQueries, fetchedItems);
      try {
        const shouldRefine = platform === "All" && fetchedItems.length < 8;
        const refinedQueries = shouldRefine ? await refineQueriesFromResults(keyword, platform, fetchedItems) : [];
        if (refinedQueries.length) {
          searchQueries = uniqueStrings([keyword, ...searchQueries, ...plannedQueries, ...refinedQueries]).slice(0, 4);
          const expandedItems = await fetchByPlatform(keyword, platform, searchQueries);
          fetchedItems = dedupeItems([...fetchedItems, ...expandedItems]);
          fetchedItems = await ensureYouTubePresence(platform, searchQueries, fetchedItems);
        } else {
          searchQueries = uniqueStrings([keyword, ...searchQueries, ...plannedQueries]).slice(0, 6);
        }
      } catch (error) {
        console.warn("OpenAI result refinement skipped:", error.message);
        searchQueries = uniqueStrings([keyword, ...searchQueries, ...plannedQueries]).slice(0, 6);
      }
      const rawItems = filterRelevantItems(fetchedItems, keyword, searchQueries);
      const fallbackTopics = buildMergedTopics(keyword, platform, rawItems);
      let items = rawItems;

      try {
        const analyzedTopics = await analyzeTrendTopics(keyword, platform, rawItems);
        if (analyzedTopics?.length) {
          items = finalizeTopicList(analyzedTopics, 4);
          if (items.length < 5) {
            items = mergeTopicLists(items, fallbackTopics, 4);
          }
        } else {
          items = finalizeTopicList(fallbackTopics, 4);
        }
      } catch (error) {
        console.warn("OpenAI analysis skipped:", error.message);
        items = finalizeTopicList(fallbackTopics, 4);
      }

      if (items.length < 8) {
        items = mergeTopicLists(items, fallbackTopics, 4);
      }

      const now = new Date().toISOString();
      saveTrackedSearch(keyword, platform, now);
      saveTopicSnapshots(keyword, platform, items, now);
      items = enrichItemsWithHistory(items);
      const payload = { items, searchQueries };
      writeCachedSearch(keyword, platform, payload);
      sendJson(res, 200, payload);
    } catch (error) {
      const storedRows = getRecentTopicsForSearch(keyword, platform, 8);
      if (storedRows.length) {
        const items = enrichItemsWithHistory(finalizeTopicList(hydrateStoredTopics(storedRows, platform), 4));
        const payload = {
          items,
          searchQueries: [keyword],
          warning: "Live APIs are limited, so showing the latest stored topic snapshot."
        };
        writeCachedSearch(keyword, platform, payload);
        sendJson(res, 200, payload);
        return;
      }

      sendJson(res, 500, { error: simplifyApiErrorMessage(error.message || "Request failed") });
    }
    return;
  }

  if (req.method === "GET" && requestUrl.pathname === "/") {
    fs.readFile(indexPath, (error, content) => {
      if (error) {
        res.writeHead(500, { "Content-Type": "text/plain; charset=utf-8" });
        res.end("Failed to load index.html");
        return;
      }

      res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
      res.end(content);
    });
    return;
  }

  res.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" });
  res.end("Not found");
});

async function refreshTrackedSearches() {
  const trackedSearches = getTrackedSearches(4);
  for (const search of trackedSearches) {
    try {
      let queries = [search.keyword];
      try {
        queries = await expandSearchQueries(search.keyword, search.platform);
      } catch {}
      const fetchedItems = await fetchByPlatform(search.keyword, search.platform, queries);
      const rawItems = filterRelevantItems(fetchedItems, search.keyword, queries);
      const fallbackTopics = buildMergedTopics(search.keyword, search.platform, rawItems);
      const items = finalizeTopicList(fallbackTopics, 4);
      const now = new Date().toISOString();
      saveTrackedSearch(search.keyword, search.platform, now);
      saveTopicSnapshots(search.keyword, search.platform, items, now);
    } catch (error) {
      console.warn("Background refresh skipped:", error.message);
    }
  }
}

server.listen(port, () => {
  console.log(`Server running at http://localhost:${port}`);
});

setInterval(() => {
  refreshTrackedSearches().catch(error => console.warn("Refresh loop failed:", error.message));
}, 30 * 60 * 1000);
