# -*- coding: utf-8 -*-
"""Lightweight, dependency-free RSS and article metadata extraction for IXCLY."""
from __future__ import annotations

import html
import json
import re
from html.parser import HTMLParser


def _plain(value: str, limit: int = 12000) -> str:
    value = html.unescape(str(value or ""))
    value = re.sub(r"<[^>]+>", " ", value)
    value = re.sub(r"https?://\S+", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value[:limit]


class _TextOnly(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts = []

    def handle_data(self, data):
        if data and data.strip():
            self.parts.append(data.strip())


def _html_text(raw: str) -> str:
    parser = _TextOnly()
    try:
        parser.feed(str(raw or ""))
        return _plain(" ".join(parser.parts))
    except Exception:
        return _plain(raw)


def extract_rss_text(entry) -> str:
    """Return the best available RSS/Atom description/content, without its markup."""
    candidates = []
    summary = getattr(entry, "summary", None) or getattr(entry, "description", None)
    if summary:
        candidates.append(_html_text(summary))
    for item in (getattr(entry, "content", None) or []):
        if isinstance(item, dict):
            value = item.get("value") or item.get("content") or ""
            if value:
                candidates.append(_html_text(value))
    # Feed parsers may expose a content:encoded field in the entry map.
    try:
        encoded = entry.get("content:encoded") or entry.get("content_encoded")
        if encoded:
            candidates.append(_html_text(encoded))
    except Exception:
        pass
    candidates = [x for x in candidates if x]
    return max(candidates, key=len)[:12000] if candidates else ""


def _walk_jsonld(value):
    if isinstance(value, list):
        for item in value:
            yield from _walk_jsonld(item)
    elif isinstance(value, dict):
        yield value
        graph = value.get("@graph")
        if graph is not None:
            yield from _walk_jsonld(graph)
        for key in ("mainEntity", "mainEntityOfPage", "subjectOf"):
            if isinstance(value.get(key), (dict, list)):
                yield from _walk_jsonld(value[key])


class _ArticleParser(HTMLParser):
    """Small HTML parser: metadata + paragraphs inside article/main/articleBody."""
    SKIP = {"script", "style", "nav", "aside", "footer", "header", "form", "noscript", "svg"}

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.meta = {}
        self.jsonld = []
        self.title_parts = []
        self.paragraphs = []
        self._title_depth = 0
        self._article_depth = 0
        self._main_depth = 0
        self._body_depth = 0
        self._skip_depth = 0
        self._p_depth = 0
        self._p_parts = []
        self._script_jsonld = False
        self._script_parts = []

    def handle_starttag(self, tag, attrs):
        a = {str(k).lower(): (v or "") for k, v in attrs}
        tag = tag.lower()
        if tag == "meta":
            key = (a.get("property") or a.get("name") or a.get("itemprop") or "").lower()
            if key and a.get("content"):
                self.meta.setdefault(key, a["content"])
        elif tag == "script" and "ld+json" in a.get("type", "").lower():
            self._script_jsonld = True
            self._script_parts = []
        if tag == "title":
            self._title_depth += 1
        if tag in self.SKIP:
            self._skip_depth += 1
        if not self._skip_depth:
            if tag == "article":
                self._article_depth += 1
            if tag == "main":
                self._main_depth += 1
            if a.get("itemprop", "").lower() == "articlebody":
                self._body_depth += 1
            if tag == "p" and (self._article_depth or self._main_depth or self._body_depth):
                self._p_depth += 1
                self._p_parts = []

    def handle_data(self, data):
        if not data:
            return
        if self._script_jsonld:
            self._script_parts.append(data)
            return
        if self._skip_depth:
            return
        if self._title_depth:
            self.title_parts.append(data.strip())
        if self._p_depth:
            self._p_parts.append(data.strip())

    def close(self):
        super().close()
        # Script close events are handled separately by HTMLParser's callbacks below.

    def handle_startendtag(self, tag, attrs):
        self.handle_starttag(tag, attrs)
        self.handle_endtag(tag)

    def handle_entityref(self, name):
        pass

    def handle_charref(self, name):
        pass

    def handle_endtag(self, tag):
        tag = tag.lower()
        if tag == "script" and self._script_jsonld:
            raw = "".join(self._script_parts).strip()
            if raw:
                self.jsonld.append(raw)
            self._script_jsonld = False
            self._script_parts = []
        if tag == "title" and self._title_depth:
            self._title_depth -= 1
        if tag == "p" and self._p_depth:
            text = _plain(" ".join(self._p_parts), 3000)
            if len(text) >= 25:
                self.paragraphs.append(text)
            self._p_depth -= 1
            self._p_parts = []
        if tag == "article" and self._article_depth:
            self._article_depth -= 1
        if tag == "main" and self._main_depth:
            self._main_depth -= 1
        if tag in self.SKIP and self._skip_depth:
            self._skip_depth -= 1


def parse_article_html(raw_html: str) -> dict:
    """Extract public article body and metadata. Does not bypass access controls."""
    parser = _ArticleParser()
    try:
        parser.feed(str(raw_html or ""))
        parser.close()
    except Exception:
        return {"title": "", "description": "", "body": "", "method": "html_parse_error"}

    ld_title = ld_desc = ld_body = ""
    for raw in parser.jsonld:
        try:
            data = json.loads(raw)
        except Exception:
            continue
        for obj in _walk_jsonld(data):
            typ = obj.get("@type", "")
            types = typ if isinstance(typ, list) else [typ]
            is_article = any("article" in str(t).lower() or "blogposting" in str(t).lower() for t in types)
            if not is_article and not any(k in obj for k in ("articleBody", "headline")):
                continue
            ld_title = ld_title or _plain(obj.get("headline") or obj.get("name") or "", 300)
            ld_desc = ld_desc or _plain(obj.get("description") or "", 1200)
            body = obj.get("articleBody")
            if isinstance(body, str):
                ld_body = ld_body or _plain(body, 12000)

    title = ld_title or _plain(" ".join(parser.title_parts), 300) or _plain(parser.meta.get("og:title") or parser.meta.get("twitter:title"), 300)
    description = (ld_desc or _plain(parser.meta.get("og:description") or parser.meta.get("article:description") or parser.meta.get("twitter:description") or parser.meta.get("description"), 1200))
    paragraphs = "\n".join(parser.paragraphs)
    body = ld_body if len(ld_body) >= 100 else paragraphs
    method = "jsonld_articleBody" if len(ld_body) >= 100 else "html_article_paragraphs" if len(paragraphs) >= 100 else "metadata_only"
    return {"title": title, "description": description, "body": body[:12000], "method": method}


def assess_news_coverage(text: str) -> dict:
    """Classify source coverage; absence of text is never interpreted as neutral sentiment."""
    cleaned = _plain(text, 12000)
    size = len(cleaned)
    if size >= 500:
        return {"level": "full", "confidence": "high", "text_chars": size}
    if size >= 180:
        return {"level": "partial", "confidence": "medium", "text_chars": size}
    if size >= 60:
        return {"level": "partial", "confidence": "low", "text_chars": size}
    return {"level": "title_only", "confidence": "low", "text_chars": size}


def cap_critical_score(score, level: str) -> int:
    """Prevent incomplete headlines from independently escalating global alert severity."""
    try:
        score = max(0, int(score))
    except (TypeError, ValueError):
        score = 0
    if level == "title_only":
        return min(score, 2)
    if level == "partial":
        return min(score, 4)
    return score


def assess_crypto_news(title: str, summary: str, domain: str = "") -> dict:
    """Editorial relevance/quality gate for crypto news; no unsupported impact claims."""
    text = f"{title or ''} {summary or ''}".lower()
    crypto_terms = (
        "bitcoin", "биткоин", "btc", "ethereum", "эфир", "crypto", "криптовалют", "крипторын",
        "binance", "coinbase", "exchange", "бирж", "etf", "defi", "stablecoin", "стейблкоин",
        "token", "токен", "altcoin", "альткоин", "open interest", "funding rate", "ликвидац",
        "liquidation", "token unlock", "разблокиров", "on-chain", "ончейн", "whale", "кит",
        "майнинг", "mining", "digital asset", "цифров актив", "wallet", "кошелёк", "sec", "cftc",
        "solana", "xrp", "bnb", "usdt", "usdc", "hack", "взлом", "exploit", "reserve", "резерв",
    )
    excluded = ("celebrity", "знаменит", "football", "футбол", "chatgpt", "нейросет", "artificial intelligence", "искусственный интеллект")
    trusted = ("coindesk.com", "cointelegraph.com", "decrypt.co", "cryptoslate.com", "bitcoinmagazine.com",
               "blockworks.co", "forklog.com", "bits.media", "theblock.co", "reuters.com", "bloomberg.com")
    matches = sorted({term for term in crypto_terms if term in text})
    trusted_source = any(domain == d or str(domain).endswith("." + d) for d in trusted)
    score = min(5, len(matches)) * 12 + (20 if trusted_source else 0) + (10 if len(summary or "") >= 350 else 0)
    unrelated = any(term in text for term in excluded)
    if unrelated and not matches:
        score -= 50
    score = max(0, min(100, score))
    return {"score": score, "crypto_relevant": bool(matches), "relevant_terms": matches[:8],
            "source_trusted": trusted_source, "publish": bool(matches) and score >= 38}
