#!/usr/bin/env python3
"""
StreamHub News, live-headlines POC server (internal demo only).

Model: headline + short excerpt + source + thumbnail, then the tap goes to the
publisher's own page. We never store or show the article body.

- Fetches each publisher's OWN public RSS feed (no scraping, no Google News RSS).
- Keeps only title, link, date, a short excerpt (max 160 chars) and the feed's thumbnail URL.
- Thumbnails are hot-linked from the publisher; nothing is copied to our server.
- Groups the same story across sources (simple word overlap) for "Top stories".

Run:  python3 server.py     then open http://localhost:8787
No packages needed (Python 3.9+ standard library only).
"""
import gzip
import html
import json
import re
import sys
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parent
CACHE_FILE = ROOT / "cache" / "last-feed.json"
PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8787
TTL = 10 * 60           # refresh feeds at most every 10 minutes
EXCERPT = 160           # max excerpt length shown on a card
UA = "StreamHubNewsPOC/0.1 (internal prototype; headline + link only)"

# group: ne = Nepali-language, en = Nepal English, world = openly licensed international
SOURCES = [
    # Nepali
    dict(id="ok",   name="Onlinekhabar",      group="ne", color="#d9232d", url="https://www.onlinekhabar.com/feed",            home="https://www.onlinekhabar.com"),
    dict(id="rp",   name="Ratopati",          group="ne", color="#c8102e", url="https://www.ratopati.com/feed",                home="https://www.ratopati.com"),
    dict(id="ng",   name="Nagarik News",      group="ne", color="#0b5aa6", url="https://nagariknews.nagariknetwork.com/feed",  home="https://nagariknews.nagariknetwork.com"),
    dict(id="np",   name="Nepal Press",       group="ne", color="#1f6f43", url="https://www.nepalpress.com/feed/",             home="https://www.nepalpress.com"),
    dict(id="kh",   name="Khabarhub",         group="ne", color="#e85d04", url="https://khabarhub.com/feed/",                  home="https://khabarhub.com"),
    dict(id="sp",   name="Setopati",          group="ne", color="#444b55", url="https://www.setopati.com/feed",                home="https://www.setopati.com"),
    dict(id="gp",   name="Gorkhapatra",       group="ne", color="#8a1c1c", url="https://gorkhapatraonline.com/rss",            home="https://gorkhapatraonline.com"),
    dict(id="bm",   name="Bizmandu",          group="ne", color="#155e75", url="https://bizmandu.com/feed",                    home="https://bizmandu.com"),
    dict(id="ap",   name="Annapurna Post",    group="ne", color="#7c2d12", url="https://annapurnapost.com/rss/",               home="https://annapurnapost.com"),
    dict(id="uj",   name="Ujyaalo",           group="ne", color="#a16207", url="https://www.ujyaaloonline.com/feed",           home="https://www.ujyaaloonline.com"),
    # Nepal, English
    dict(id="oke",  name="Onlinekhabar English", group="en", color="#d9232d", url="https://english.onlinekhabar.com/feed",     home="https://english.onlinekhabar.com"),
    dict(id="tkp",  name="The Kathmandu Post", group="en", color="#111827", url="https://kathmandupost.com/rss",               home="https://kathmandupost.com"),
    dict(id="rep",  name="Republica",         group="en", color="#0b5aa6", url="https://myrepublica.nagariknetwork.com/feeds", home="https://myrepublica.nagariknetwork.com"),
    dict(id="tht",  name="The Himalayan Times", group="en", color="#1e3a8a", url="https://thehimalayantimes.com/rssFeed/11",  home="https://thehimalayantimes.com"),
    dict(id="nt",   name="Nepali Times",      group="en", color="#b91c1c", url="https://nepalitimes.com/feed",                 home="https://nepalitimes.com"),
    dict(id="trn",  name="The Rising Nepal",  group="en", color="#8a1c1c", url="https://risingnepaldaily.com/rss",             home="https://risingnepaldaily.com"),
    dict(id="khe",  name="Khabarhub English", group="en", color="#e85d04", url="https://english.khabarhub.com/feed/",          home="https://english.khabarhub.com"),
    dict(id="rpe",  name="Ratopati English",  group="en", color="#c8102e", url="https://english.ratopati.com/feed",            home="https://english.ratopati.com"),
    # World (openly licensed / public-interest sources)
    dict(id="un",   name="UN News",           group="world", color="#009edb", url="https://news.un.org/feed/subscribe/en/news/all/rss.xml", home="https://news.un.org"),
    dict(id="gv",   name="Global Voices",     group="world", color="#3a8d3f", url="https://globalvoices.org/feed/",            home="https://globalvoices.org"),
    dict(id="tc",   name="The Conversation",  group="world", color="#d8352a", url="https://theconversation.com/global/articles.atom", home="https://theconversation.com"),
]
# Publishers' official YouTube channels (public channel feeds; played only through YouTube's own embed player).
CHANNELS = [
    dict(id="yt-ok",  name="Onlinekhabar",        group="ne",    color="#d9232d", cid="UCo4cuctdb-1YdZNgWEVZGwA"),
    dict(id="yt-kn",  name="Kantipur",            group="ne",    color="#c41e3a", cid="UCH690VR64topganWjVKivzg"),
    dict(id="yt-rp",  name="Ratopati TV",         group="ne",    color="#c8102e", cid="UC1ev3ii9O4RuUVZ-5mfOBQQ"),
    dict(id="yt-np",  name="Nepal Press",         group="ne",    color="#1f6f43", cid="UCK-Qwd5DsMG2Y8nPkLECvFQ"),
    dict(id="yt-sp",  name="Setopati",            group="ne",    color="#444b55", cid="UCPjHEeafcMdr-balIMiEwrg"),
    dict(id="yt-ng",  name="Nagarik Network",     group="ne",    color="#0b5aa6", cid="UCxxx4M3jP9HcKLHJ0dFLe7g"),
    dict(id="yt-kh",  name="Khabarhub",           group="ne",    color="#e85d04", cid="UC6jGO_6WZOBTNa5YhaiVBgA"),
    dict(id="yt-ap",  name="Annapurna Post",      group="ne",    color="#7c2d12", cid="UCxMMePKBj8N_pjmsUJ3wOyQ"),
    dict(id="yt-uj",  name="Ujyaalo TV",          group="ne",    color="#a16207", cid="UCnDBWS2a4gKxEjEYsrDiQIw"),
    dict(id="yt-gp",  name="Gorkhapatra TV",      group="ne",    color="#8a1c1c", cid="UCFbhgZG_y9soBNx8nFrKl3g"),
    dict(id="yt-ntv", name="Nepal Television",    group="ne",    color="#b45309", cid="UCTGVQIvtPu5kqNI5ABmN8Fw"),
    dict(id="yt-bbn", name="BBC News Nepali",     group="ne",    color="#b80000", cid="UCa9wigEw31xDMh_nrscmL6g"),
    dict(id="yt-tkp", name="The Kathmandu Post",  group="en",    color="#111827", cid="UCdQwir9haF67rLaD3I92zxA"),
    dict(id="yt-tht", name="The Himalayan Times", group="en",    color="#1e3a8a", cid="UCwR2lvIyX2uefblp51G5x_A"),
    dict(id="yt-nt",  name="Nepali Times",        group="en",    color="#b91c1c", cid="UCjgmTKy-e_zrpmdnzXQ3pBA"),
    dict(id="yt-dw",  name="DW News",             group="world", color="#0a64a0", cid="UCknLrEdhRCp1aegoMqRaCZg"),
    dict(id="yt-aj",  name="Al Jazeera English",  group="world", color="#c88a12", cid="UCNye-wNBqNL5ZzHSJj3l8Bg"),
    dict(id="yt-bbc", name="BBC News",            group="world", color="#b80000", cid="UC16niRr50-MSBwiO3YDb3RA"),
    dict(id="yt-f24", name="France 24 English",   group="world", color="#00a3e0", cid="UCQfwfsi5VrQ8yKZ-UWmAEFg"),
    dict(id="yt-un",  name="United Nations",      group="world", color="#009edb", cid="UC5O114-PQNYkurlTg6hekZw"),
]
# Republica's YouTube channel is left out (no uploads since April 2025).
# Left out on purpose: ekantipur (terms ban bots/scripts; no feed), Google News RSS (personal use only).

_lock = threading.Lock()
_state = {"at": 0, "data": None}


# ---------- lenient feed parsing (several Nepali feeds are malformed XML) ----------
def _clean(s):
    if not s:
        return ""
    s = re.sub(r"<!\[CDATA\[(.*?)\]\]>", r"\1", s, flags=re.S)
    s = html.unescape(s)
    s = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", s, flags=re.S | re.I)
    s = re.sub(r"<[^>]+>", " ", s)
    s = html.unescape(s)  # double-escaped feeds
    return re.sub(r"\s+", " ", s).strip()


def _tag(block, *names):
    for n in names:
        m = re.search(rf"<{n}(?:\s[^>]*)?>(.*?)</{n}>", block, flags=re.S | re.I)
        if m and m.group(1).strip():
            return m.group(1)
    return ""


def _attr(block, tag, attr):
    m = re.search(rf"<{tag}\b[^>]*\b{attr}\s*=\s*[\"']([^\"']+)[\"']", block, flags=re.I)
    return html.unescape(m.group(1)) if m else ""


def _image(block):
    for tag, attr in (("media:thumbnail", "url"), ("media:content", "url"), ("enclosure", "url")):
        u = _attr(block, tag, attr)
        if u and (tag != "enclosure" or re.search(r"\.(jpe?g|png|webp|gif)", u, re.I) or "image" in block):
            return u
    raw = _tag(block, "image")
    if raw:
        raw = _clean(raw) if "<url>" not in raw else _clean(_tag(raw, "url"))
        if raw.startswith("http"):
            return raw
    for field in ("description", "content:encoded", "content", "summary"):
        m = re.search(r"<img[^>]+src=[\"']([^\"']+)", html.unescape(_tag(block, field)), flags=re.I)
        if m:
            return m.group(1)
    return ""


def _date(block):
    raw = _clean(_tag(block, "pubDate", "dc:date", "published", "updated"))
    if not raw:
        return None
    try:
        d = parsedate_to_datetime(raw)
    except Exception:
        try:
            d = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except Exception:
            return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc).isoformat()


def _excerpt(text, title):
    t = _clean(text)
    if t.startswith(title):
        t = t[len(title):].strip(" -:|")
    t = re.sub(r"\[\s*(…|\.\.\.|&hellip;)\s*\]|The post .*? appeared first on .*$", "", t).strip()
    if len(t) <= EXCERPT:
        return t
    cut = t[:EXCERPT]
    cut = cut[: max(cut.rfind(" "), cut.rfind("।"), 100)]
    return cut.rstrip(" ,;:–-") + "…"


def parse_feed(xml, src):
    items = re.findall(r"<item\b.*?</item>", xml, flags=re.S | re.I) or re.findall(r"<entry\b.*?</entry>", xml, flags=re.S | re.I)
    out = []
    for b in items[:30]:
        title = _clean(_tag(b, "title"))
        link = _clean(_tag(b, "link")) or _attr(b, "link", "href")
        if not title or not link.startswith("http"):
            continue
        out.append({
            "id": f"{src['id']}:{abs(hash(link)) % 10**10}",
            "src": src["id"],
            "title": title,
            "excerpt": _excerpt(_tag(b, "description", "summary") or "", title),
            "url": link,
            "img": _image(b),
            "published": _date(b),
            "lang": "ne" if re.search(r"[ऀ-ॿ]", title) else "en",
        })
    return out


def fetch(src):
    t0 = time.time()
    try:
        req = urllib.request.Request(src["url"], headers={"User-Agent": UA, "Accept": "application/rss+xml, application/xml, text/xml, */*"})
        with urllib.request.urlopen(req, timeout=12) as r:
            raw = r.read()
        if raw[:2] == b"\x1f\x8b":  # some servers gzip even when not asked
            raw = gzip.decompress(raw)
        xml = raw.decode("utf-8", errors="replace").lstrip()
        items = parse_feed(xml, src)
        return src["id"], items, {"ok": bool(items), "count": len(items), "ms": int((time.time() - t0) * 1000), "error": None if items else "no items parsed"}
    except Exception as e:
        return src["id"], [], {"ok": False, "count": 0, "ms": int((time.time() - t0) * 1000), "error": type(e).__name__ + ": " + str(e)[:120]}


# ---------- "same story" grouping (very simple: shared words in the headline) ----------
STOP = set("the a an and of to in on for at by with from is are was be as after over new says say said its it this that nepal nepali news".split())
STOP |= set("र को का की ले मा छ हो भने पनि गर्न गरे गरेको लागि सम्म तथा वा यो त्यो भएको हुने छन् थियो एक दुई तीन जना जनाको गरी बाट".split())


def _words(t):
    ws = re.findall(r"[ऀ-ॿ]{2,}|[A-Za-z]{3,}|\d{3,}", t.lower())
    return {w for w in ws if w not in STOP}


def cluster(items):
    recent = [i for i in items if i["published"]]
    recent.sort(key=lambda i: i["published"], reverse=True)
    recent = recent[:400]
    groups = []
    for it in recent:
        w = _words(it["title"])
        if len(w) < 2:
            continue
        best = None
        for g in groups:
            if it["src"] in g["srcs"] or g["lang"] != it["lang"]:
                continue
            # compare with each headline already in the group (not the growing union, which over-attracts)
            score = max(len(w & m) / max(1, min(len(w), len(m))) for m in g["members"])
            inter = max(len(w & m) for m in g["members"])
            if inter >= 3 and score >= 0.5 or inter >= 2 and score >= 0.67:
                best = g
                break
        if best:
            best["items"].append(it["id"])
            best["srcs"].add(it["src"])
            best["members"].append(w)
        else:
            groups.append({"items": [it["id"]], "srcs": {it["src"]}, "members": [w], "lang": it["lang"]})
    multi = [g for g in groups if len(g["srcs"]) >= 2]
    multi.sort(key=lambda g: len(g["srcs"]), reverse=True)
    return [{"items": g["items"], "sources": len(g["srcs"]), "lang": g["lang"]} for g in multi[:12]]


def fetch_channel(ch):
    t0 = time.time()
    try:
        url = f"https://www.youtube.com/feeds/videos.xml?channel_id={ch['cid']}"
        with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": UA}), timeout=12) as r:
            raw = r.read()
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        xml = raw.decode("utf-8", errors="replace")
        out = []
        for b in re.findall(r"<entry>.*?</entry>", xml, flags=re.S):
            vid = _clean(_tag(b, "yt:videoId"))
            href = _attr(b, "link", "href")
            views = _attr(b, "media:statistics", "views")
            if not vid:
                continue
            out.append({
                "id": vid, "ch": ch["id"], "title": _clean(_tag(b, "title")),
                "published": _date(b), "short": "/shorts/" in href,
                "thumb": _attr(b, "media:thumbnail", "url") or f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg",
                "views": int(views) if views.isdigit() else None,
                "url": href or f"https://www.youtube.com/watch?v={vid}",
                "lang": "ne" if re.search(r"[\u0900-\u097F]", _clean(_tag(b, "title"))) else "en",
            })
        return ch["id"], out, {"ok": bool(out), "count": len(out), "shorts": sum(v["short"] for v in out), "ms": int((time.time() - t0) * 1000), "error": None if out else "no entries"}
    except Exception as e:
        return ch["id"], [], {"ok": False, "count": 0, "shorts": 0, "ms": int((time.time() - t0) * 1000), "error": type(e).__name__ + ": " + str(e)[:120]}


def build():
    with ThreadPoolExecutor(max_workers=12) as ex:
        results = list(ex.map(fetch, SOURCES))
        vresults = list(ex.map(fetch_channel, CHANNELS))
    items, status = [], {}
    for sid, its, st in results:
        items += its
        status[sid] = st
    seen, uniq = set(), []
    for i in items:
        if i["url"] in seen:
            continue
        seen.add(i["url"])
        uniq.append(i)
    uniq.sort(key=lambda i: i["published"] or "", reverse=True)
    data = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "sources": [{k: s[k] for k in ("id", "name", "group", "color", "home")} | status[s["id"]] for s in SOURCES],
        "items": uniq,
        "clusters": cluster(uniq),
        "channels": [{k: c[k] for k in ("id", "name", "group", "color", "cid")} | st for c, (_, _, st) in zip(CHANNELS, vresults)],
        "videos": sorted([v for _, vs, _ in vresults for v in vs], key=lambda v: v["published"] or "", reverse=True),
    }
    CACHE_FILE.parent.mkdir(exist_ok=True)
    CACHE_FILE.write_text(json.dumps(data, ensure_ascii=False))
    return data


def get_data(force=False):
    with _lock:
        if force or not _state["data"] or time.time() - _state["at"] > TTL:
            try:
                _state["data"], _state["at"] = build(), time.time()
            except Exception as e:  # network down → last good snapshot
                if CACHE_FILE.exists() and not _state["data"]:
                    _state["data"] = json.loads(CACHE_FILE.read_text())
                    _state["data"]["stale"] = str(e)
        return _state["data"]


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *a, **kw):
        super().__init__(*a, directory=str(ROOT / "web"), **kw)

    def do_GET(self):
        if self.path.startswith("/api/feed"):
            body = json.dumps(get_data(force="refresh=1" in self.path), ensure_ascii=False).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        return super().do_GET()

    def log_message(self, fmt, *args):
        if "/api/" in (args[0] if args else ""):
            sys.stderr.write("%s\n" % (fmt % args))


if __name__ == "__main__":
    print(f"Fetching feeds from {len(SOURCES)} publishers…")
    d = get_data()
    ok = sum(1 for s in d["sources"] if s["ok"])
    print(f"{ok}/{len(SOURCES)} feeds OK · {len(d['items'])} headlines · {len(d['clusters'])} multi-source stories")
    vok = sum(1 for c in d["channels"] if c["ok"])
    print(f"{vok}/{len(CHANNELS)} YouTube channels OK · {sum(not v['short'] for v in d['videos'])} videos · {sum(v['short'] for v in d['videos'])} shorts")
    print(f"Open http://localhost:{PORT}")
    ThreadingHTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
