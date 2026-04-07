const fs = require("fs");
const path = require("path");

const dataDir = path.resolve(__dirname, "..", "data");
const dbPath = path.join(dataDir, "trends.json");

if (!fs.existsSync(dataDir)) {
  fs.mkdirSync(dataDir, { recursive: true });
}

function readStore() {
  if (!fs.existsSync(dbPath)) {
    return { topicSnapshots: [], trackedSearches: [] };
  }

  try {
    return JSON.parse(fs.readFileSync(dbPath, "utf8"));
  } catch {
    return { topicSnapshots: [], trackedSearches: [] };
  }
}

function writeStore(store) {
  fs.writeFileSync(dbPath, JSON.stringify(store, null, 2), "utf8");
}

function saveTrackedSearch(keyword, platform, timestamp) {
  const store = readStore();
  const existing = store.trackedSearches.find(item => item.keyword === keyword && item.platform === platform);
  if (existing) {
    existing.last_seen_at = timestamp;
  } else {
    store.trackedSearches.push({ keyword, platform, last_seen_at: timestamp });
  }
  writeStore(store);
}

function saveTopicSnapshots(keyword, platform, items, timestamp) {
  const store = readStore();
  const rows = items
    .filter(item => item.topicKey)
    .map(item => ({
      search_keyword: keyword,
      search_platform: platform,
      topic_key: item.topicKey,
      topic_title: item.title || item.rawTitle || "Untitled topic",
      dominant_platform: item.platform || platform,
      source_link: item.firstSourceLink || item.link || "",
      recorded_at: timestamp,
      published_at: item.firstSourceTime || item.time || timestamp,
      score: Math.round(Number(item.score || 0)),
      trust_score: Math.round(Number(item.trustBreakdown?.totalTrustScore || 0)),
      source_count: Math.max(1, Number(item.metrics?.sources || item.matchedSources?.length || 1)),
      views: Math.round(Number(item.metrics?.views || 0)),
      likes: Math.round(Number(item.metrics?.likes || 0)),
      comments: Math.round(Number(item.metrics?.comments || 0))
    }));

  store.topicSnapshots.push(...rows);
  if (store.topicSnapshots.length > 4000) {
    store.topicSnapshots = store.topicSnapshots.slice(-4000);
  }
  writeStore(store);
}

function getTopicHistory(topicKey) {
  const store = readStore();
  return store.topicSnapshots
    .filter(item => item.topic_key === topicKey)
    .sort((a, b) => a.recorded_at.localeCompare(b.recorded_at))
    .slice(-60);
}

function getTrackedSearches(limit = 6) {
  const store = readStore();
  return store.trackedSearches
    .sort((a, b) => b.last_seen_at.localeCompare(a.last_seen_at))
    .slice(0, limit);
}

function getRecentTopicsForSearch(keyword, platform, limit = 8) {
  const store = readStore();
  const normalizedKeyword = String(keyword || "").trim().toLowerCase();
  const normalizedPlatform = String(platform || "All").trim();

  const rows = store.topicSnapshots
    .filter(item => String(item.search_keyword || "").trim().toLowerCase() === normalizedKeyword)
    .filter(item => normalizedPlatform === "All" || String(item.search_platform || "").trim() === normalizedPlatform)
    .sort((a, b) => {
      if (b.recorded_at !== a.recorded_at) return String(b.recorded_at).localeCompare(String(a.recorded_at));
      return Number(b.score || 0) - Number(a.score || 0);
    });

  const latestByTopic = new Map();
  for (const row of rows) {
    if (!latestByTopic.has(row.topic_key)) {
      latestByTopic.set(row.topic_key, row);
    }
  }

  return [...latestByTopic.values()]
    .sort((a, b) => Number(b.score || 0) - Number(a.score || 0))
    .slice(0, limit);
}

module.exports = {
  dbPath,
  getRecentTopicsForSearch,
  getTopicHistory,
  getTrackedSearches,
  saveTopicSnapshots,
  saveTrackedSearch
};
