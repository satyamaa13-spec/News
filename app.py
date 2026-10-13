"""
India News Aggregator
Fetches RSS from several outlets, groups the same story, summarises, links back.

Run:
    pip install flask feedparser trafilatura
    python app.py            ->  http://localhost:5000

Optional better summaries (needs Anthropic key):
    pip install anthropic
    export ANTHROPIC_API_KEY=sk-...
"""
import os, re, time, html, json, threading, datetime, urllib.request, urllib.parse, urllib.error
from concurrent.futures import ThreadPoolExecutor
import feedparser
try:
    import trafilatura          # extracts full article text from a URL
except ImportError:
    trafilatura = None
from flask import Flask, render_template_string

# ---- Edit this list. Verify each URL still works (outlets change feeds). ----
FEEDS = {
    # --- English: national ---
    "NDTV":              "https://feeds.feedburner.com/ndtvnews-top-stories",
    "Times of India":    "https://timesofindia.indiatimes.com/rssfeedstopstories.cms",
    "Hindustan Times":   "https://www.hindustantimes.com/feeds/rss/india-news/rssfeed.xml",
    "The Hindu":         "https://www.thehindu.com/news/national/feeder/default.rss",
    "Indian Express":    "https://indianexpress.com/section/india/feed/",
    "India Today":       "https://www.indiatoday.in/rss/1206578",
    "News18":            ["https://www.news18.com/commonfeeds/v1/eng/rss/india.xml", "https://www.news18.com/rss/india.xml"],
    "Firstpost":         ["https://www.firstpost.com/commonfeeds/v1/mfp/rss/india.xml", "https://www.firstpost.com/india/feed/", "https://www.firstpost.com/feed/"],
    "The Print":         ["https://theprint.in/feed/", "https://theprint.in/category/india/feed/"],
    "Scroll.in":         "https://feeds.feedburner.com/ScrollinArticles.rss",
    # --- Business ---
    "Economic Times":    "https://economictimes.indiatimes.com/rssfeedstopstories.cms",
    "Mint":              "https://www.livemint.com/rss/news",
    "Business Standard": ["https://www.business-standard.com/rss/home_page_top_stories.rss", "https://www.business-standard.com/rss/latest.rss", "https://www.business-standard.com/rss/india-news-216.rss"],
    # --- International view on India ---
    "BBC News":          "https://feeds.bbci.co.uk/news/world/asia/india/rss.xml",
}
FAILED = set()   # outlets whose feed returned nothing on the last refresh
PER_FEED = 25          # items taken per outlet
SIMILARITY = 0.34      # headline overlap needed to treat as same story
PER_CATEGORY = 6       # max stories shown per category (full articles are downloaded for these)
CACHE_SECONDS = 600    # refresh every 10 minutes

STOP = set("a an the of in on at to for and or is are was were be by with from as it its this that "
           "after over says say said new news live updates update latest today".split())

def clean(t):
    t = re.sub(r"<[^>]+>", " ", html.unescape(t or ""))
    return re.sub(r"\s+", " ", t).strip()

def tokens(title):
    return {w for w in re.findall(r"\w+", title.lower()) if w not in STOP and len(w) > 2}

HEADERS = {  # look like a normal browser; several outlets block Python's default
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "application/rss+xml, application/xml, text/xml, */*",
    "Accept-Language": "en-IN,en;q=0.9",
}

# Last-resort fallback for every outlet: Google News RSS filtered to that site.
# Always works, but gives headlines only (summaries then rely on the article page).
DOMAINS = {
    "NDTV": "ndtv.com", "Times of India": "timesofindia.indiatimes.com",
    "Hindustan Times": "hindustantimes.com", "The Hindu": "thehindu.com",
    "Indian Express": "indianexpress.com", "India Today": "indiatoday.in",
    "News18": "news18.com", "Firstpost": "firstpost.com", "The Print": "theprint.in",
    "Scroll.in": "scroll.in", "Economic Times": "economictimes.indiatimes.com",
    "Mint": "livemint.com", "Business Standard": "business-standard.com",
    "BBC News": "bbc.com",
}

def gnews_url(name):
    return (f"https://news.google.com/rss/search?q=site:{DOMAINS[name]}+when:1d"
            "&hl=en-IN&gl=IN&ceid=IN:en")

def get_feed(url):
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=20) as r:
        return feedparser.parse(r.read())

def fetch_one(item):
    name, urls = item
    urls = [urls] if isinstance(urls, str) else list(urls)
    if name in DOMAINS:
        urls.append(gnews_url(name))
    for url in urls:
        try:
            d = get_feed(url)
        except Exception as ex:
            print(f"[{name}] {url} -> {ex}")
            continue
        out = []
        for e in d.entries[:PER_FEED]:
            title = clean(e.get("title"))
            if not title:
                continue
            if "news.google.com" in url:   # Google appends " - Outlet" to titles
                title = re.sub(r"\s+-\s+[^-]+$", "", title)
            out.append({"source": name, "title": title, "link": e.get("link", "#"),
                        "desc": clean(e.get("summary") or e.get("description")),
                        "ts": e.get("published_parsed") or e.get("updated_parsed")})
        if out:
            FAILED.discard(name)
            print(f"[{name}] OK ({len(out)} items) {url}")
            return out
        print(f"[{name}] {url} -> no items")
    FAILED.add(name)
    return []

def cluster(items):
    groups = []
    for it in items:
        tk = tokens(it["title"])
        if not tk:
            continue
        it["tk"] = tk
        best, score = None, 0
        for g in groups:
            if any(m["source"] == it["source"] for m in g["items"]):
                continue  # one article per outlet per story
            s = len(tk & g["tk"]) / len(tk | g["tk"])
            if s > score:
                best, score = g, s
        if best and score >= SIMILARITY:
            best["items"].append(it)
            best["tk"] |= tk
        else:
            groups.append({"items": [it], "tk": set(tk)})
    return groups

def extractive_summary(g):
    descs = [i["desc"] for i in g["items"] if len(i["desc"]) > 40]
    if not descs:
        return g["items"][0]["title"]
    text = max(descs, key=len)
    return text[:280].rsplit(" ", 1)[0] + ("…" if len(text) > 280 else "")

def fetch_article(url):
    """Download the article page and return its main text ('' on failure)."""
    if not trafilatura:
        return ""
    try:
        raw = trafilatura.fetch_url(url)
        return (trafilatura.extract(raw, include_comments=False, include_tables=False) or "") if raw else ""
    except Exception:
        return ""

def sentences(text):
    return [x.strip() for x in re.split(r"(?<=[.!?।])\s+", re.sub(r"\s+", " ", text)) if len(x.strip()) > 25]

def item_summary(i, limit=450):
    """Summary from full article text if we have it (else the RSS blurb).
    Built from whole sentences, never cut mid-sentence."""
    text = i.get("text") or i["desc"]
    if len(text) < 40:
        return i["title"]
    out = ""
    for sent in sentences(text):
        if out and len(out) + len(sent) > limit:
            break
        out += (" " if out else "") + sent
    return out or text[:limit]

def ai_summaries(groups):
    """Optional: for multi-outlet stories, Claude writes a separate English summary per outlet
    plus a note on how the coverage differs (angle, emphasis, tone, omissions)."""
    if not os.getenv("ANTHROPIC_API_KEY"):
        return
    try:
        import anthropic, json
        client = anthropic.Anthropic()
        for g in groups:
            if len(g["items"]) < 2:
                continue
            src = "\n".join(f"[{i['source']}] {i['title']}. {(i.get('text') or i['desc'])[:5000]}" for i in g["items"])
            r = client.messages.create(
                model="claude-haiku-5-5", max_tokens=2000,
                messages=[{"role": "user", "content":
                    "Below are headlines/snippets from different outlets on the same story. "
                    "Reply with ONLY JSON: {\"outlets\": {\"<outlet name>\": \"a 2-3 sentence summary of "
                    "what THAT outlet reports: key facts, who said what, and its angle\"}, "
                    "\"difference\": \"2-3 neutral sentences "
                    "on how the framing, emphasis or details differ between outlets (say if they "
                    "essentially agree)\"}. Use only the text given; do not invent details or "
                    "attribute bias beyond what the wording shows. Write in English only.\n\n" + src}])
            data = json.loads(re.sub(r"```json|```", "", r.content[0].text).strip())
            for i in g["items"]:
                if data.get("outlets", {}).get(i["source"]):
                    i["summary"] = data["outlets"][i["source"]]
            g["difference"] = data.get("difference", "")
    except Exception as ex:
        print("AI summary skipped:", ex)

# ---- Categories: (id, label, keywords). Edit freely. ----
CATS = [
    ("politics", "Politics", ["bjp", "congress", "modi", "parliament", "election", "elections", "minister",
        "chief minister", "lok sabha", "rajya sabha", "assembly", "opposition", "rahul gandhi", "kejriwal",
        "mamata", "poll", "polls", "cabinet", "amit shah", "nda", "aap", "tmc", "mla", "mp", "bill", "govt",
        "government", "vote", "voters", "campaign", "party", "leader", "yogi", "nitish", "stalin"]),
    ("international", "International", ["u.s", "usa", "america", "american", "washington", "china",
        "chinese", "pakistan", "russia", "russian", "ukraine", "israel", "gaza", "iran", "united nations", "unsc",
        "unga", "trump", "putin", "biden", "white house", "bangladesh", "nepal", "sri lanka", "britain", "uk",
        "europe", "european", "eu", "nato", "taliban", "afghanistan", "canada", "australia", "saudi", "uae",
        "foreign", "embassy", "tariff", "tariffs", "beijing", "moscow", "london", "paris"]),
    ("finance", "Finance & Markets", ["sensex", "nifty", "stock", "stocks", "share", "shares",
        "rbi", "rupee", "inflation", "gdp", "bank", "banks", "interest rate", "repo rate", "ipo", "mutual fund",
        "tax", "taxes", "budget", "gold", "silver", "crude", "oil prices", "market", "markets", "sebi", "dollar",
        "bond", "bonds", "loan", "loans", "gst", "fiscal", "forex", "investors", "crypto", "bitcoin"]),
    ("business", "Business", ["company", "companies", "ceo", "startup", "startups", "merger",
        "acquisition", "profit", "revenue", "earnings", "quarterly", "tata", "reliance", "adani", "infosys", "wipro",
        "airline", "airlines", "automaker", "layoffs", "funding", "sales", "investment", "industry", "firm",
        "brand", "retail", "exports", "imports", "manufacturing", "ambani", "indigo", "air india", "q1", "q2",
        "q3", "q4"]),
    ("sports", "Sports", ["cricket", "ipl", "bcci", "match", "test", "odi", "t20", "football", "fifa",
        "olympic", "olympics", "tennis", "hockey", "kohli", "rohit", "wicket", "goal", "tournament",
        "championship", "medal", "badminton", "world cup", "asia cup", "coach", "squad", "batter", "bowler",
        "league", "wrestler", "athlete", "grand slam", "formula 1", "f1", "kabaddi", "chess", "icc"]),
    ("entertainment", "Entertainment", ["bollywood", "film", "films", "movie", "movies", "actor",
        "actress", "trailer", "box office", "web series", "ott", "netflix", "singer", "song", "album",
        "bigg boss", "director", "celebrity", "kapoor", "salman", "shah rukh", "deepika", "concert", "series",
        "tollywood", "kollywood", "award", "awards"]),
    ("tech", "Technology", ["ai", "artificial intelligence", "iphone", "apple", "google", "microsoft",
        "smartphone", "app", "software", "cyber", "cyberattack", "isro", "satellite", "chip", "semiconductor",
        "5g", "openai", "tech", "chatgpt", "samsung", "meta", "x corp", "data breach", "robot", "launch vehicle",
        "gadget", "laptop", "android"]),
    ("science", "Science, Health & Environment", ["health", "hospital", "disease",
        "virus", "vaccine", "cancer", "doctor", "doctors", "covid", "who", "study", "research", "scientists",
        "nasa", "climate", "heatwave", "monsoon", "rain", "rains", "flood", "floods", "earthquake", "cyclone",
        "weather", "imd", "pollution", "aqi", "dengue", "medical", "patients", "wildlife", "forest", "landslide"]),
    ("crime", "Crime & Law", ["police", "arrested", "arrest", "murder", "court", "supreme court",
        "high court", "verdict", "bail", "cbi", "fir", "accused", "rape", "killed", "probe", "investigation", "jail",
        "enforcement directorate", "judge", "lawsuit", "fraud", "scam", "stabbed", "shot", "custody", "trial",
        "chargesheet", "terror", "terrorist", "encounter"]),
]
OTHER = ("other", "Other News")
BUSINESS_OUTLETS = {"Economic Times", "Mint", "Business Standard"}
_CAT_RX = {c[0]: re.compile(r"\b(?:" + "|".join(re.escape(k) for k in c[2]) + r")\b") for c in CATS}

def categorize(g):
    """Pick a category from keywords in the English headlines (and English blurbs)."""
    title = " ".join(i["title"] for i in g["items"]).lower()
    desc = " ".join(i["desc"] for i in g["items"]).lower()
    best, best_score = OTHER[0], 0
    for cid, _, _ in CATS:
        rx = _CAT_RX[cid]
        score = 2 * len(set(rx.findall(title))) + len(set(rx.findall(desc)))
        if cid in ("business", "finance") and any(i["source"] in BUSINESS_OUTLETS for i in g["items"]):
            score += 1                      # business outlets nudge toward business/finance
        if score > best_score:
            best, best_score = cid, score
    return best

_cache = {"t": 0, "data": []}
_lock = threading.Lock()

def build():
    FAILED.clear()
    with ThreadPoolExecutor(max_workers=len(FEEDS)) as ex:
        items = [i for batch in ex.map(fetch_one, FEEDS.items()) for i in batch]
    groups = cluster(items)
    for g in groups:
        g["count"] = len(g["items"])
    groups.sort(key=lambda g: -g["count"])          # most widely covered first
    for g in groups:
        g["cat"] = categorize(g)
    top, per = [], {}
    for g in groups:                                 # best-covered stories first, capped per category
        if per.get(g["cat"], 0) < PER_CATEGORY:
            top.append(g)
            per[g["cat"]] = per.get(g["cat"], 0) + 1
    arts = [i for g in top for i in g["items"]]
    print(f"downloading {len(arts)} articles...")
    with ThreadPoolExecutor(max_workers=16) as ex:   # download full articles in parallel
        for i, t in zip(arts, ex.map(lambda x: fetch_article(x["link"]), arts)):
            i["text"] = t
    for g in top:
        g["difference"] = ""
        for i in g["items"]:
            i["summary"] = item_summary(i)
    print("summarising...")
    ai_summaries(top)
    for g in top:
        g["title"] = g["items"][0]["title"]
    return top

def get_data():
    with _lock:
        if time.time() - _cache["t"] > CACHE_SECONDS:
            _cache.update(t=time.time(), data=build())
        return _cache["data"]

PAGE = """<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Nazariya</title>
<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Source+Serif+4:opsz,wght@8..60,400;8..60,600;8..60,700&family=Fraunces:opsz,wght@9..144,800&family=Tiro+Devanagari+Hindi&display=swap" rel="stylesheet">
<style>
:root{--bg:#fdfdfb;--ink:#1a1a1a;--body:#333;--mut:#6b6b66;--rule:#dcdcd6;--acc:#8f2d1f}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--ink);font:17px/1.55 "Source Serif 4",Georgia,serif}
.wrap{max-width:740px;margin:0 auto;padding:0 20px}
.ui{font-family:system-ui,-apple-system,"Segoe UI",sans-serif}
header{padding-top:30px}
.mast{display:flex;align-items:center;gap:16px}
.mark{width:76px;height:48px;flex-shrink:0;color:var(--acc)}
.brand{display:flex;flex-direction:column;line-height:1}
h1{margin:0;font-family:"Fraunces",Georgia,serif;font-weight:800;font-size:3.2rem;letter-spacing:-.03em;line-height:1}
h1 i{font-style:normal;color:var(--acc)}
.dev{font-family:"Tiro Devanagari Hindi",serif;font-size:1.15rem;color:var(--acc);margin-top:7px;letter-spacing:.04em}
.dateline{margin:12px 0 0;padding-bottom:10px;border-bottom:2px solid var(--ink);font-size:.85rem;color:var(--mut)}
nav{position:sticky;top:0;z-index:5;background:var(--bg);border-bottom:1px solid var(--rule);padding:10px 0 0}
.search{position:relative}
.search input{width:100%;font:inherit;font-size:.95rem;padding:6px 26px 6px 0;border:0;border-bottom:1px solid var(--rule);
 background:transparent;color:var(--ink);outline:none;border-radius:0}
.search input:focus{border-bottom-color:var(--ink)}
.search input::placeholder{color:#999}
.search input::-webkit-search-cancel-button{display:none}
.clr{position:absolute;right:0;top:50%;transform:translateY(-50%);border:0;background:none;color:var(--mut);
 font-size:1.2rem;line-height:1;cursor:pointer;padding:2px 4px;display:none}
.clr:hover{color:var(--ink)}
.cats{display:flex;flex-wrap:wrap;gap:0 18px;margin-top:4px}
.chip{font:inherit;font-size:.86rem;padding:9px 0 8px;border:0;border-bottom:2px solid transparent;background:none;
 color:var(--mut);cursor:pointer;white-space:nowrap}
.chip:hover{color:var(--ink)}
.chip.on{color:var(--ink);border-bottom-color:var(--ink);font-weight:600}
.chip small{margin-left:4px;font-size:.78rem;opacity:.7}
main{padding:6px 0 60px}
#none{display:none;color:var(--mut);padding:30px 0}
.sec.gone,.card.hide{display:none}
.sech{margin:34px 0 0;padding-bottom:6px;font-size:1.05rem;font-weight:700;border-bottom:1px solid var(--ink)}
.card{padding:18px 0 20px;border-bottom:1px solid var(--rule)}
.card:last-child{border-bottom:0}
.card h2{margin:0 0 4px;font-size:1.3rem;line-height:1.25;font-weight:700}
.meta{font-size:.8rem;color:var(--mut);margin:0 0 6px}
.tabs{display:flex;flex-wrap:wrap;gap:4px 16px;margin:10px 0 8px;border-bottom:1px solid var(--rule)}
.tab{font:inherit;font-size:.83rem;padding:4px 0 6px;margin-bottom:-1px;border:0;border-bottom:2px solid transparent;
 background:none;color:var(--mut);cursor:pointer}
.tab:hover{color:var(--ink)}
.tab.on{color:var(--ink);border-bottom-color:var(--acc);font-weight:600}
.panel{display:none;margin-top:8px}
.panel.on{display:block}
.otitle{font-weight:600;font-size:1rem;margin-bottom:2px}
.panel p{margin:0 0 8px;color:var(--body)}
.panel a{font-size:.85rem;color:var(--acc);text-decoration:none;border-bottom:1px solid currentColor}
.panel a:hover{color:var(--ink)}
.diff{margin:14px 0 0;font-size:.95rem;color:var(--body)}
.diff em{font-weight:600}
footer{padding:0 0 40px;color:var(--mut);font-size:.8rem}
@media(max-width:560px){nav{position:static}body{font-size:16px}h1{font-size:2.4rem}.mark{width:56px;height:35px}.card h2{font-size:1.2rem}}
</style></head><body>
<div class="wrap">
<header>
  <div class="mast">
  <svg class="mark" viewBox="0 0 64 40" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" aria-hidden="true">
  <path d="M2 20C13 4 51 4 62 20C51 36 13 36 2 20Z"/><circle cx="32" cy="20" r="8" fill="currentColor" stroke="none"/>
  <path d="M32 20L2 20M32 20L10 8M32 20L10 32" stroke="var(--bg)" stroke-width="1.6" opacity=".9"/><circle cx="35" cy="17" r="2.2" fill="var(--bg)" stroke="none"/></svg>
  <div class="brand"><h1>Nazar<i>iya</i></h1><span class="dev">नज़रिया</span></div>
  </div>
  <p class="dateline ui">{{ today }} &middot; {{ n }} stories from {{ srcs }} outlets</p>
</header>
<nav class="ui">
  <div class="search">
    <input type="search" id="q" placeholder="Search" autocomplete="off" aria-label="Search stories">
    <button type="button" class="clr" id="clr" aria-label="Clear search">&times;</button>
  </div>
  <div class="cats">
  <button type="button" class="chip on" data-c="all">All<small>{{ n }}</small></button>
  {% for s in sections %}<button type="button" class="chip" data-c="{{ s.id }}">{{ s.label }}<small>{{ s.groups|length }}</small></button>{% endfor %}
  </div>
</nav>
<main>
<div id="none" class="ui">Nothing matches that search.</div>
{% for s in sections %}
<section class="sec" data-c="{{ s.id }}">
<h2 class="sech">{{ s.label }}</h2>
{% for g in s.groups %}
<article class="card">
  <h2>{{ g.title }}</h2>
  {% if g.count > 1 %}<p class="meta ui">Reported by {{ g.count }} outlets</p>
  <div class="tabs ui">{% for i in g["items"] %}
    <button type="button" class="tab{% if loop.first %} on{% endif %}" data-i="{{ loop.index0 }}">{{ i.source }}</button>{% endfor %}
  </div>{% endif %}
  {% for i in g["items"] %}
  <div class="panel{% if loop.first %} on{% endif %}" data-i="{{ loop.index0 }}">
    {% if i.title != g.title %}<div class="otitle">{{ i.title }}</div>{% endif %}
    <p>{{ i.summary }}</p>
    <a class="ui" href="{{ i.link }}" target="_blank" rel="noopener">{{ i.source }}, full story &rarr;</a>
  </div>{% endfor %}
  {% if g.difference %}<p class="diff"><em>Where they differ.</em> {{ g.difference }}</p>{% endif %}
</article>
{% endfor %}
</section>
{% endfor %}
</main>
{% if failed %}<footer class="ui">Couldn't load: {{ failed|join(', ') }}</footer>{% endif %}
</div>
<script>
var qEl=document.getElementById("q"),clr=document.getElementById("clr"),none=document.getElementById("none");
function apply(){
  var on=document.querySelector(".chip.on"),cat=on?on.dataset.c:"all";
  var terms=qEl.value.toLowerCase().split(" ").filter(Boolean),shown=0;
  clr.style.display=qEl.value?"block":"none";
  document.querySelectorAll(".sec").forEach(function(sec){
    var any=0;
    sec.querySelectorAll(".card").forEach(function(c){
      var t=c.textContent.toLowerCase();
      var ok=terms.every(function(w){return t.indexOf(w)!==-1;});
      c.classList.toggle("hide",!ok);
      if(ok) any++;
    });
    var inCat=cat==="all"||sec.dataset.c===cat;
    sec.classList.toggle("gone",!inCat||any===0);
    if(inCat) shown+=any;
  });
  none.style.display=shown?"none":"block";
}
qEl.addEventListener("input",apply);
qEl.addEventListener("keydown",function(e){if(e.key==="Escape"){qEl.value="";apply();}});
clr.addEventListener("click",function(){qEl.value="";apply();qEl.focus();});
document.addEventListener("click",function(e){
  var ch=e.target.closest(".chip");
  if(ch){
    document.querySelectorAll(".chip").forEach(function(x){x.classList.toggle("on",x===ch);});
    apply();
    window.scrollTo({top:0});
    return;
  }
  var b=e.target.closest(".tab"); if(!b) return;
  b.closest(".card").querySelectorAll(".tab,.panel").forEach(function(x){
    x.classList.toggle("on",x.dataset.i===b.dataset.i);
  });
});
</script>
</body></html>"""

app = Flask(__name__)

# Build the first batch in the background at startup so the first visitor doesn't wait for it.
threading.Thread(target=get_data, daemon=True).start()

@app.route("/")
def index():
    g = get_data()
    sections = []
    for cid, label in [(c[0], c[1]) for c in CATS] + [OTHER]:
        gs = [x for x in g if x["cat"] == cid]
        if gs:
            sections.append({"id": cid, "label": label, "groups": gs})
    d = datetime.date.today()
    today = f"{d:%A}, {d.day} {d:%B %Y}"
    return render_template_string(PAGE, sections=sections, n=len(g), today=today,
                                  srcs=len(FEEDS) - len(FAILED), failed=sorted(FAILED))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
