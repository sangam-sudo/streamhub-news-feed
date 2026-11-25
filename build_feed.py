"""Builds the news feed once and writes it to public/api/feed.json for GitHub Pages."""
import json
from pathlib import Path

import server

OUT = Path(__file__).resolve().parent / "public" / "api" / "feed.json"

data = server.build()
OUT.parent.mkdir(parents=True, exist_ok=True)
OUT.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")

ok = sum(1 for s in data["sources"] if s["ok"])
print(f"{ok}/{len(data['sources'])} feeds OK, {len(data['items'])} headlines, "
      f"{len(data['clusters'])} stories, {len(data['videos'])} videos -> {OUT}")
