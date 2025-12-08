from datetime import datetime

from bs4 import BeautifulSoup
from readability import Document


def parse_html(url: str, html: str) -> dict:
    soup = BeautifulSoup(html, "lxml")

    # Title
    if soup.title and soup.title.string:
        title = soup.title.string.strip()
    else:
        title = ""

    # Use Readability to get main content
    doc = Document(html)
    main_html = doc.summary()
    main_soup = BeautifulSoup(main_html, "lxml")

    # Remove junk tags
    for tag in main_soup(["script", "style", "noscript"]):
        tag.decompose()

    content = main_soup.get_text(separator=" ", strip=True)

    # H1
    h1_tag = soup.find("h1")
    h1_text = h1_tag.get_text(strip=True) if h1_tag else ""

    # Meta tags
    meta_desc = ""
    meta_keywords = ""

     

    return {
        "url": url,
        "title": title,
        "content": content,
        "content_length": len(content),
        "h1": h1_text,
        "meta_description": meta_desc,
        "meta_keywords": meta_keywords,
        "crawled_at": datetime.utcnow().isoformat(),
    }
