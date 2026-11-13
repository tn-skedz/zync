# language: Python, file: main.py, target: FastAPI + Python 3.10+
# Multi-source resolver: JioSaavn (full 320kbps) + YouTube (yt-dlp fallback).

from __future__ import annotations
import asyncio, urllib.parse
from typing import Any
import os

import httpx
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles

APP_NAME = "openwave"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")

app = FastAPI(title="OpenWave")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])

# ---------- host allowlist for the proxy (SSRF guard) ----------
ALLOWED_HOSTS = (
    "saavncdn.com", "aac.saavncdn.com", 
    "googlevideo.com", "youtube.com", "ytimg.com"
)

def _host_ok(url: str) -> bool:
    try:
        h = urllib.parse.urlparse(url).hostname or ""
    except Exception:
        return False
    return any(h == a or h.endswith("." + a) for a in ALLOWED_HOSTS)

# ---------- JioSaavn ----------
JIOSAAVN_HOSTS = [
    "https://saavn.dev",
    "https://jiosavan-api-with-playlist.vercel.app",
]

def _pick_best(urls: list[dict]) -> str | None:
    by_q: dict[str, str] = {u.get("quality"): u.get("url") for u in urls if u.get("url")}
    for q in ("320kbps", "160kbps", "96kbps", "48kbps", "12kbps"):
        if q in by_q:
            return by_q[q]
    return next(iter(by_q.values()), None)

async def search_jiosaavn(client: httpx.AsyncClient, q: str, limit: int) -> list[dict]:
    payload: Any = None
    for host in JIOSAAVN_HOSTS:
        try:
            r = await client.get(f"{host}/api/search/songs",
                                 params={"query": q, "limit": limit})
            r.raise_for_status()
            payload = r.json()
            break
        except Exception:
            continue
    if not payload:
        return []

    results = payload.get("data", {}).get("results", [])
    out: list[dict] = []
    for s in results:
        best = _pick_best(s.get("downloadUrl") or [])
        if not best:
            continue
        imgs = s.get("image") or []
        cover = imgs[-1].get("url") if imgs else ""
        artists = s.get("artists", {}).get("primary", []) or []
        artist = ", ".join(a.get("name", "") for a in artists) or "Unknown"
        out.append({
            "id": s.get("id"),
            "title": s.get("name", ""),
            "artist": artist,
            "album": (s.get("album") or {}).get("name", ""),
            "cover": cover,
            "source": "jiosaavn",
            "play": "/api/stream?src=jiosaavn&u=" + urllib.parse.quote(best, safe=""),
        })
    return out

# ---------- YouTube via yt-dlp ----------
def _yt_search_sync(q: str, n: int) -> list[dict]:
    import yt_dlp
    # Add cookiefile if it exists to bypass bot-checks
    cookie_path = "cookies.txt" if os.path.exists("cookies.txt") else None
    
    opts = {
        "quiet": True, 
        "skip_download": True,
        "extract_flat": True, 
        "noplaylist": True,
    }
    if cookie_path:
        opts["cookiefile"] = cookie_path

    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(f"ytsearch{n}:{q}", download=False)
    out: list[dict] = []
    for e in (info or {}).get("entries", []) or []:
        if not e:
            continue
        thumbs = e.get("thumbnails") or []
        out.append({
            "id": e["id"],
            "title": e.get("title", ""),
            "artist": e.get("uploader", "YouTube"),
            "album": "",
            "cover": thumbs[-1].get("url", "") if thumbs else "",
            "source": "youtube",
            "play": f"/api/stream?src=youtube&id={e['id']}",
        })
    return out

async def search_youtube(q: str, limit: int) -> list[dict]:
    try:
        return await asyncio.to_thread(_yt_search_sync, q, limit)
    except Exception:
        return []

# ---------- /api/search ----------
@app.get("/api/search")
async def api_search(q: str = Query(..., min_length=1),
                     limit: int = 20, yt: int = 1):
    async with httpx.AsyncClient(headers={"User-Agent": UA}, timeout=20) as client:
        tasks = [
            search_jiosaavn(client, q, limit),
        ]
        if yt:
            tasks.append(search_youtube(q, max(6, limit // 3)))
        gathered = await asyncio.gather(*tasks, return_exceptions=True)

    merged: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for chunk in gathered:
        if isinstance(chunk, Exception):
            continue
        for item in chunk:
            key = (item["title"].lower().strip(), item["artist"].lower().strip())
            if key in seen:
                continue
            seen.add(key)
            merged.append(item)
    return {"query": q, "count": len(merged), "results": merged}

# ---------- /api/stream (Range-aware proxy) ----------
async def _proxy_upstream(upstream_url: str, request: Request) -> StreamingResponse:
    if not _host_ok(upstream_url):
        raise HTTPException(400, "host not allowed")

    headers: dict[str, str] = {"User-Agent": UA}
    rng = request.headers.get("range")
    if rng:
        headers["Range"] = rng

    client = httpx.AsyncClient(timeout=None, follow_redirects=True)
    try:
        req = client.build_request("GET", upstream_url, headers=headers)
        resp = await client.send(req, stream=True)
    except Exception:
        await client.aclose()
        raise HTTPException(502, "upstream unreachable")

    if resp.status_code >= 400:
        await resp.aclose()
        await client.aclose()
        raise HTTPException(resp.status_code, "upstream error")

    out_headers: dict[str, str] = {}
    for k in ("content-type", "content-length", "content-range", "accept-ranges"):
        v = resp.headers.get(k)
        if v:
            out_headers[k] = v

    async def pipe():
        try:
            async for chunk in resp.aiter_raw(8192):
                yield chunk
        finally:
            await resp.aclose()
            await client.aclose()

    return StreamingResponse(pipe(), status_code=resp.status_code, headers=out_headers)

@app.get("/api/stream")
async def api_stream(request: Request, src: str,
                     u: str | None = None, id: str | None = None):
    if src == "jiosaavn":
        if not u:
            raise HTTPException(400, "missing u")
        return await _proxy_upstream(u, request)

    if src == "youtube":
        if not id:
            raise HTTPException(400, "missing id")
        def _resolve() -> str:
            import yt_dlp
            cookie_path = "cookies.txt" if os.path.exists("cookies.txt") else None
            
            opts = {"quiet": True, "skip_download": True, "format": "bestaudio/best"}
            if cookie_path:
                opts["cookiefile"] = cookie_path
                
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(
                    f"https://www.youtube.com/watch?v={id}", download=False)
            return info["url"]
        try:
            url = await asyncio.to_thread(_resolve)
        except Exception:
            raise HTTPException(502, "yt resolve failed")
        return await _proxy_upstream(url, request)

    raise HTTPException(400, "unknown src")

@app.get("/api/health")
async def health():
    return {"ok": True, "app": APP_NAME}

# Serve the static UI. 
# Ensure your folder structure has main.py in root, and a 'web' folder next to it.
if os.path.exists("web"):
    app.mount("/", StaticFiles(directory="web", html=True), name="web")
else:
    print("WARNING: 'web' directory not found. UI will not load.")