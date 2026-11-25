# StreamHub news feed

Temporary host for the StreamHub News feed until the StreamHub backend serves it.

A GitHub Actions workflow runs every 15 minutes: `build_feed.py` fetches the publishers' RSS feeds and
YouTube channel feeds (`server.py`), keeps headlines, excerpts of at most 160 characters and links, groups the
same story from several publishers, and publishes the result with GitHub Pages:

    https://sangam-sudo.github.io/streamhub-news-feed/api/feed.json

Headline + source + link-out only. No article bodies are stored or served.

Run locally: `python3 build_feed.py` (writes `public/api/feed.json`), or `python3 server.py` for the local dev server.
