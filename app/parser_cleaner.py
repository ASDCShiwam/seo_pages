from datetime import datetime, timezone

from bs4 import BeautifulSoup
from readability import Document


def _extract_meta_tag(soup: BeautifulSoup, names: list[str], props: list[str] | None = None) -> str:
    """
    Try multiple <meta> name / property combinations and return the first non-empty content.
    """
    # name="...":
    for name in names:
        tag = soup.find("meta", attrs={"name": name})
        if tag and tag.get("content"):
            return tag["content"].strip()

    # property="...":
    if props:
        for prop in props:
            tag = soup.find("meta", attrs={"property": prop})
            if tag and tag.get("content"):
                return tag["content"].strip()

    return ""


def _looks_like_code_or_css(line: str) -> bool:
    """
    Heuristic to drop lines that are mostly JS/CSS/code instead of human-readable text.
    This is to avoid storing CSS/JS junk inside 'content'.
    """
    if not line:
        return False

    # Very long single-line blocks are often minified code
    if len(line) > 400:
        return True

    # Ratio of special characters to total length
    special = sum(not c.isalnum() and not c.isspace() for c in line)
    ratio = special / len(line)

    # If a line has many special chars and is not tiny, treat as code
    if len(line) > 80 and ratio > 0.35:
        return True

    # Typical JS/CSS/code patterns
    code_keywords = (
        "function ",
        "var ",
        "let ",
        "const ",
        "=>",
        "if(",
        "for(",
        "while(",
        "return ",
        "{",
        "}",
        ";",
        "/*",
        "*/",
        ".class",
        "background:",
        "color:",
        "margin:",
        "padding:",
    )

    hits = sum(1 for kw in code_keywords if kw in line)
    if hits >= 3:
        return True

    return False


def parse_html(url: str, html: str) -> dict:
    """
    Parse raw HTML into a clean, SEO-friendly document:
    - Removes <script>, <style>, <noscript>, <link> etc. from the main content.
    - Filters out lines that look like JS/CSS/code.
    - Extracts title, meta description/keywords, H1, headings, canonical URL.
    - Adds a short summary field based on main content.
    """
    soup = BeautifulSoup(html, "lxml")

    # ---- TITLE ----
    if soup.title and soup.title.string:
        title = soup.title.string.strip()
    else:
        # Fallback on og:title if present
        og_title_tag = soup.find("meta", attrs={"property": "og:title"})
        title = og_title_tag.get("content", "").strip() if og_title_tag else ""

    # ---- META DESCRIPTION / KEYWORDS ----
    meta_desc = _extract_meta_tag(
        soup,
        names=["description"],
        props=["og:description", "twitter:description"],
    )

    meta_keywords = _extract_meta_tag(
        soup,
        names=["keywords"],
        props=None,
    )

    # ---- LANGUAGE ----
    html_tag = soup.find("html")
    lang = ""
    if html_tag is not None:
        lang = html_tag.get("lang", "").strip()

    # ---- CANONICAL URL ----
    canonical_url = ""
    canonical_tag = soup.find("link", rel=lambda x: x and "canonical" in x.lower())
    if canonical_tag and canonical_tag.get("href"):
        canonical_url = canonical_tag["href"].strip()

    # ---- MAIN CONTENT USING READABILITY ----
    doc = Document(html)
    main_html = doc.summary()
    main_soup = BeautifulSoup(main_html, "lxml")

    # Remove obvious junk: JS/CSS/etc.
    for tag_name in ("script", "style", "noscript"):
        for tag in main_soup.find_all(tag_name):
            tag.decompose()

    # Remove CSS/JS <link> or similar
    for tag in main_soup.find_all("link"):
        tag.decompose()

    # Optionally remove nav/footer/forms to keep core content
    for tag in main_soup.find_all(["nav", "footer"]):
        tag.decompose()

    # Get text lines
    raw_lines = [
        line.strip()
        for line in main_soup.get_text(separator="\n").splitlines()
        if line.strip()
    ]

    # Filter out lines that look like code, JS, or CSS
    filtered_lines = [ln for ln in raw_lines if not _looks_like_code_or_css(ln)]

    # Join back into one string (space-separated to avoid giant single line)
    content = " ".join(filtered_lines)
    content = " ".join(content.split())  # collapse multiple spaces

    # Content length
    content_length = len(content)

    # ---- HEADINGS ----
    h1_tags = [h.get_text(strip=True) for h in soup.find_all("h1")]
    h2_tags = [h.get_text(strip=True) for h in soup.find_all("h2")]
    h3_tags = [h.get_text(strip=True) for h in soup.find_all("h3")]

    primary_h1 = h1_tags[0] if h1_tags else ""

    # ---- SUMMARY (for quick preview / fallback meta description) ----
    # If meta description is empty, use first 160 chars of main content
    if not meta_desc and content:
        meta_desc = content[:160]

    summary = content[:250] if content else ""

    # ---- FINAL DOCUMENT ----
    return {
        "url": url,
        "canonical_url": canonical_url or url,
        "title": title,
        "content": content,
        "content_length": content_length,
        "summary": summary,
        "h1": primary_h1,
        "headings_h1": h1_tags,
        "headings_h2": h2_tags,
        "headings_h3": h3_tags,
        "meta_description": meta_desc,
        "meta_keywords": meta_keywords,
        "lang": lang,
        "crawled_at": datetime.now(timezone.utc).isoformat(),
    }
