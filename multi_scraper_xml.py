# multi_scraper_xml.py
# HTML-scraper for NemTilmeld-lister der genererer:
#  - 1 XML i NemTilmeld-lignende format pr. site: out/data-<host>.xml
#  - 1 RSS pr. site: out/rss-<host>.xml
#  - Samle-XML: data_all.xml
#  - Samle-RSS: out/rss-all.xml
#
# Kræver: requests, beautifulsoup4, lxml
# Kilder hentes fra sources.txt (en URL pr. linje). Både root-URL og /events/ accepteres.

from __future__ import annotations
import os
import re
import sys
import time
import logging
from datetime import datetime, timezone
from urllib.parse import urlparse, urljoin

import requests
from bs4 import BeautifulSoup
from lxml import etree as ET

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# ---------- HTTP session ----------
S = requests.Session()
S.headers.update({
    "User-Agent": "ScleroseForeningen-FeedBuilder/1.0 (+https://scleroseforeningen.dk)",
    "Accept-Language": "da,da-DK;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Connection": "close",
})

# ---------- helpers ----------
MONTHS_DA = {
    "januar": 1, "februar": 2, "marts": 3, "april": 4, "maj": 5, "juni": 6,
    "juli": 7, "august": 8, "september": 9, "oktober": 10, "november": 11, "december": 12
}

def host_of(url: str) -> str:
    return urlparse(url).netloc

def root_of(url: str) -> str:
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}/"

def to_listing_url(url: str) -> str:
    """
    Normaliser en kilde-URL til /events/-listen (ikke feeds).
    """
    url = url.strip()
    if not url:
        raise ValueError("Tom URL")
    p = urlparse(url)
    if not p.scheme:
        url = "https://" + url
        p = urlparse(url)
    # Hvis allerede /events/ eller /events/list/... -> brug /events/
    if p.path.endswith("/events/") or p.path.endswith("/events"):
        return f"{p.scheme}://{p.netloc}/events/"
    # hvis root
    if p.path in ("", "/"):
        return f"{p.scheme}://{p.netloc}/events/"
    # ellers klip til /events/
    return f"{p.scheme}://{p.netloc}/events/"

def load_sources(cli_args: list[str]) -> list[str]:
    """
    Kilder kan gives som CLI-argumenter; hvis ingen, læses sources.txt
    """
    if cli_args:
        return cli_args
    path = "sources.txt"
    if not os.path.exists(path):
        logging.warning("sources.txt findes ikke – ingen kilder.")
        return []
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if s and not s.startswith("#"):
                out.append(s)
    return out

def fetch(url: str, retries: int = 3, timeout: int = 30, allow_redirects: bool = True) -> requests.Response:
    """
    Robust GET med exponential backoff. Rejser sidste fejl,
    men kaldes i try/except i main() så én dårlig side ikke vælter alt.
    """
    last_err = None
    for i in range(retries):
        try:
            r = S.get(url, timeout=timeout, allow_redirects=allow_redirects)
            r.raise_for_status()
            return r
        except Exception as e:
            last_err = e
            sleep = 2 ** i
            logging.warning("HTTP-fejl på %s (%s). Forsøger igen om %ss ...", url, e, sleep)
            time.sleep(sleep)
    raise last_err

def text(el) -> str:
    return (el.get_text(separator=" ", strip=True) if el else "").strip()

def as_aware_utc(dt: datetime | None) -> datetime | None:
    """Normalisér til UTC-aware (bruges til sortering / pubDate i RSS)."""
    if dt is None:
        return None
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)

def parse_dk_datetime(blob: str) -> datetime | None:
    """
    Forsøg at parse noget à la:
      'Tirsdag d. 2. september 2025 kl. 19:00'
      '2. september 2025 kl. 19:00'
      '2025-09-14 10:00'
    Returnerer naive datetime (senere konverteret til aware i RSS).
    """
    if not blob:
        return None
    b = blob.lower().strip()

    # ISO-ish først
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})[ T](\d{1,2}):(\d{2})", b)
    if m:
        y, mo, d, hh, mm = map(int, m.groups())
        try:
            return datetime(y, mo, d, hh, mm)
        except ValueError:
            return None

    # Dansk form: 2. september 2025 kl. 19:00
    m = re.search(
        r"(\d{1,2})\.\s*([a-zæøå]+)\s*(\d{4}).*?kl\.?\s*(\d{1,2})(?::|\.)(\d{2})",
        b, re.IGNORECASE
    )
    if m:
        d, mname, y, hh, mm = m.groups()
        d, y, hh, mm = int(d), int(y), int(hh), int(mm)
        mo = MONTHS_DA.get(mname, 0)
        if mo:
            try:
                return datetime(y, mo, d, hh, mm)
            except ValueError:
                return None

    # Hvis dato uden tid
    m = re.search(r"(\d{1,2})\.\s*([a-zæøå]+)\s*(\d{4})", b, re.IGNORECASE)
    if m:
        d, mname, y = m.groups()
        d, y = int(d), int(y)
        mo = MONTHS_DA.get(mname, 0)
        if mo:
            try:
                return datetime(y, mo, d, 0, 0)
            except ValueError:
                return None

    return None

# ---------- scraping ----------

EVENT_LINK_RE = re.compile(r"/\d+/?$")

def discover_event_links(listing_html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(listing_html, "html.parser")
    links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        # absolut eller relativ
        if href.startswith("http"):
            if host_of(href) == host_of(base_url) and EVENT_LINK_RE.search(urlparse(href).path or ""):
                links.add(href.split("?")[0])
        else:
            if EVENT_LINK_RE.search(href):
                links.add(urljoin(base_url, href.split("?")[0]))
    return sorted(links)

def extract_site_title(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    # <title>
    if soup.title and soup.title.string:
        return soup.title.string.strip()
    # fallback: brand/logo alt text
    img = soup.find("img", {"title": True}) or soup.find("img", {"alt": True})
    if img:
        return img.get("title") or img.get("alt") or ""
    return ""

def parse_event_teaser(card_el: BeautifulSoup) -> str:
    # prøv at tage første p/div i kortet
    for sel in ["p", "div"]:
        c = card_el.find(sel)
        if c and text(c):
            return text(c)
    return ""

def scrape_detail(detail_url: str) -> dict:
    """
    Returner info fra detalje-siden (HTML til description, evt. tidspunkt/placering).
    """
    r = fetch(detail_url)
    html = r.text
    soup = BeautifulSoup(html, "html.parser")

    # titel
    title = ""
    if soup.find("h1"):
        title = text(soup.find("h1"))
    if not title and soup.title:
        title = soup.title.text.strip()

    # forsøg at finde en "content/description"-blok
    desc_html = ""
    candidates = [
        {"id": "event-description"}, {"class": re.compile("description|content|event__body|js-nemtilmeld_event_field-description")},
        {"id": "content"}, {"class": "content"}
    ]
    for sel in candidates:
        block = soup.find("div", sel) or soup.find("section", sel) or soup.find("article", sel)
        if block:
            desc_html = str(block)
            break
    if not desc_html:
        # fallback: main
        main = soup.find("main")
        if main:
            desc_html = str(main)
        else:
            body = soup.find("body")
            if body:
                # skrab ikke hele navigationen – men som fallback er det bedre end ingenting
                desc_html = str(body)

    # Prøv at finde en tekstdato
    possible = soup.get_text(" ", strip=True)
    dt = parse_dk_datetime(possible)

    # Lokalitet – heuristik
    location = ""
    for lab in ["Adresse", "Sted", "Lokation", "Location"]:
        m = re.search(lab + r"\s*[:\-]\s*(.+)", possible, re.IGNORECASE)
        if m:
            location = m.group(1).strip()
            break

    return {
        "title": title,
        "detail_html": desc_html,
        "start": dt,
        "location": location
    }

def scrape_listing(listing_url: str) -> tuple[str, list[dict]]:
    """
    Finder event-links på /events/ og besøger hver detalje-side for rig data.
    Returnerer (site_title, events[])
    """
    r = fetch(listing_url)
    site_title = extract_site_title(r.text)
    links = discover_event_links(r.text, listing_url)

    events = []
    for href in links:
        try:
            d = scrape_detail(href)
            # sikr titel
            if not d.get("title"):
                d["title"] = f"Arrangement ({href.rsplit('/',2)[-2]})"
            d["link"] = href
            events.append(d)
        except Exception as e:
            logging.error("Fejl ved detalje %s: %s", href, e)
            continue

    return site_title, events

# ---------- XML writers (NemTilmeld-lignende + RSS) ----------

def provider_block() -> ET.Element:
    prov = ET.Element("provider")
    for tag, val in [
        ("title", "NemTilmeld Aps"),
        ("address", "Strømmen 6"),
        ("zipcode", "9400"),
        ("city", "Nørresundby"),
        ("email", "info@nemtilmeld.dk"),
        ("phone", "+45 70404070"),
        ("website", "https://www.nemtilmeld.dk"),
    ]:
        el = ET.SubElement(prov, tag)
        el.text = ET.CDATA(val)
    return prov

def build_custom_event(ev: dict, host: str, site_title: str) -> ET.Element:
    """
    Konstruér et <event> element med et subset af felter.
    """
    # event-id fra link path
    ev_id = re.search(r"/(\d+)/?$", ev.get("link","") or "")
    ev_id = (ev_id.group(1) if ev_id else "0")

    e = ET.Element("event", id=ev_id)

    # org_event_id (holder samme id)
    ET.SubElement(e, "org_event_id").text = ev_id

    # titel
    title = ET.SubElement(e, "title")
    t = ev.get("title") or "Arrangement"
    title.text = ET.CDATA(f"{t} | {site_title or host}")

    # description (HTML)
    desc = ET.SubElement(e, "description")
    desc.text = ET.CDATA(ev.get("detail_html") or "")

    # kort beskrivelse
    short = ET.SubElement(e, "description_short")
    short.text = ET.CDATA((ev.get("teaser") or "").strip())

    # tider
    start_dt = ev.get("start")
    if isinstance(start_dt, datetime):
        start_text = start_dt.strftime("%Y-%m-%d %I:%M %p")
        start_common = start_dt.strftime("%Y-%m-%d %H:%M:%S")
    else:
        start_text = ""
        start_common = ""
    ET.SubElement(e, "start_time").text = start_text
    ET.SubElement(e, "end_time").text = ""  # ukendt fra HTML i generel form
    ET.SubElement(e, "deadline").text = ""
    ET.SubElement(e, "start_time_common").text = start_common
    ET.SubElement(e, "end_time_common").text = ""
    ET.SubElement(e, "deadline_time_common").text = ""

    # tickets (ukendt pris – tom struktur)
    tickets = ET.SubElement(e, "tickets")
    # Behold tomt – NemTilmeld-klienter tåler tom <tickets/>

    # availability (ukendt – sæt konservativt)
    ET.SubElement(e, "available_tickets").text = "true"
    ET.SubElement(e, "available_tickets_quantity").text = ""
    ET.SubElement(e, "highest_ticket_price").text = ""
    ET.SubElement(e, "few_tickets_left").text = "false"
    ET.SubElement(e, "public_status").text = "registration_open"

    # url
    ET.SubElement(e, "url").text = ev.get("link") or ""

    # images (ukendt – tom)
    ET.SubElement(e, "images")

    # categories (tom)
    ET.SubElement(e, "categories").text = " "

    # location (helt enkel – vi placerer den tekst vi kunne finde)
    loc = ET.SubElement(e, "location", id=ev_id)
    lt = ET.SubElement(loc, "type"); lt.text = ET.CDATA("address")
    ln = ET.SubElement(loc, "name"); ln.text = ET.CDATA(ev.get("title") or "Arrangement")
    la = ET.SubElement(loc, "address"); la.text = ET.CDATA("")
    lz = ET.SubElement(loc, "zipcode"); lz.text = ET.CDATA("")
    lc = ET.SubElement(loc, "city"); lc.text = ET.CDATA(ev.get("location") or "")
    lco = ET.SubElement(loc, "country"); lco.text = ET.CDATA("DK")

    # organization – vi bruger sitets navn/host
    org = ET.SubElement(e, "organization", id="0")
    for tag, val in [
        ("title", site_title or host),
        ("address", ""),
        ("zipcode", ""),
        ("city", ""),
        ("phone", ""),
        ("country", "DK"),
        ("url", f"https://{host}/"),
        ("description", ""),
        ("email", ""),
    ]:
        el = ET.SubElement(org, tag)
        el.text = ET.CDATA(val)

    # contact
    contact = ET.SubElement(e, "contact_details")
    cn = ET.SubElement(contact, "name"); cn.text = ET.CDATA(site_title or host)
    cp = ET.SubElement(contact, "phone"); cp.text = ET.CDATA("")
    ce = ET.SubElement(contact, "email"); ce.text = ET.CDATA("")

    return e

def write_custom_for_site(host: str, site_title: str, events: list[dict], out_path: str):
    data = ET.Element("data")
    data.append(provider_block())
    evs = ET.SubElement(data, "events")

    for ev in events:
        ev_el = build_custom_event(ev, host, site_title)
        evs.append(ev_el)

    xml = ET.tostring(data, encoding="utf-8", xml_declaration=True, pretty_print=True)
    with open(out_path, "wb") as f:
        f.write(xml)

def write_custom_all(all_event_elements: list[ET.Element], out_path: str):
    data = ET.Element("data")
    data.append(provider_block())
    evs = ET.SubElement(data, "events")
    for el in all_event_elements:
        evs.append(el)
    xml = ET.tostring(data, encoding="utf-8", xml_declaration=True, pretty_print=True)
    with open(out_path, "wb") as f:
        f.write(xml)

def write_rss_for_site(host: str, site_title: str, site_url: str, events: list[dict], out_path: str):
    rss = ET.Element("rss", version="2.0")
    ch = ET.SubElement(rss, "channel")
    ET.SubElement(ch, "title").text = site_title or host
    ET.SubElement(ch, "link").text = site_url
    ET.SubElement(ch, "description").text = f"Arrangementer fra {site_title or host}"
    ET.SubElement(ch, "language").text = "da-DK"

    now = datetime.now(timezone.utc)
    ET.SubElement(ch, "pubDate").text = now.strftime("%a, %d %b %Y %H:%M:%S %z")

    # ✅ sortér med UTC-aware tider
    events_sorted = sorted(events, key=lambda e: as_aware_utc(e.get("start")) or now)

    for ev in events_sorted:
        it = ET.SubElement(ch, "item")
        ET.SubElement(it, "title").text = ev.get("title") or "Arrangement"
        ET.SubElement(it, "link").text = ev.get("link") or ""
        g = ET.SubElement(it, "guid", isPermaLink="true"); g.text = ev.get("link") or ""

        desc = ET.SubElement(it, "description")
        html = ev.get("detail_html") or f"<div>{ev.get('teaser','')}</div>"
        desc.text = ET.CDATA(html)

        dt = as_aware_utc(ev.get("start")) or now
        ET.SubElement(it, "pubDate").text = dt.strftime("%a, %d %b %Y %H:%M:%S %z")

    xml = ET.tostring(rss, encoding="utf-8", xml_declaration=True, pretty_print=True)
    with open(out_path, "wb") as f:
        f.write(xml)

def write_rss_all(all_events: list[dict], out_path: str):
    rss = ET.Element("rss", version="2.0")
    ch = ET.SubElement(rss, "channel")
    ET.SubElement(ch, "title").text = "Scleroseforeningen – samlede arrangementer"
    ET.SubElement(ch, "link").text = "https://scleroseforeningen.dk/"
    ET.SubElement(ch, "description").text = "Aggregat af lokale arrangementer (HTML-scrapet)"
    ET.SubElement(ch, "language").text = "da-DK"

    now = datetime.now(timezone.utc)
    ET.SubElement(ch, "pubDate").text = now.strftime("%a, %d %b %Y %H:%M:%S %z")

    # ✅ UTC-aware sortering
    events_sorted = sorted(all_events, key=lambda e: as_aware_utc(e.get("start")) or now)

    for ev in events_sorted:
        it = ET.SubElement(ch, "item")
        ET.SubElement(it, "title").text = ev.get("title") or "Arrangement"
        ET.SubElement(it, "link").text = ev.get("link") or ""
        g = ET.SubElement(it, "guid", isPermaLink="true"); g.text = ev.get("link") or ""

        desc = ET.SubElement(it, "description")
        html = ev.get("detail_html") or f"<div>{ev.get('teaser','')}</div>"
        desc.text = ET.CDATA(html)

        dt = as_aware_utc(ev.get("start")) or now
        ET.SubElement(it, "pubDate").text = dt.strftime("%a, %d %b %Y %H:%M:%S %z")

    xml = ET.tostring(rss, encoding="utf-8", xml_declaration=True, pretty_print=True)
    with open(out_path, "wb") as f:
        f.write(xml)

# ---------- main ----------

def main(argv):
    srcs = load_sources(argv[1:])
    listing_urls = []
    for s in srcs:
        try:
            listing_urls.append(to_listing_url(s))
        except Exception as e:
            logging.warning("Springer kilde %s (%s)", s, e)

    os.makedirs("out", exist_ok=True)

    all_custom_elements = []
    all_events_flat = []
    summary = []

    if not listing_urls:
        logging.error("Ingen gyldige kilder.")
        # skriv tomme filer, så workflow ikke fejler
        write_custom_all([], "data_all.xml")
        write_rss_all([], "out/rss-all.xml")
        return 0

    for listing in listing_urls:
        host = host_of(listing)
        base = root_of(listing)
        try:
            logging.info("Scraper: %s", listing)
            site_title, events = scrape_listing(listing)

            # skriv pr. site
            write_custom_for_site(host, site_title, events, f"out/data-{host}.xml")
            write_rss_for_site(host, site_title, base, events, f"out/rss-{host}.xml")

            # til aggregater
            for ev in events:
                all_events_flat.append(ev)
                all_custom_elements.append(build_custom_event(ev, host, site_title))

            summary.append((host, len(events)))
        except Exception as e:
            logging.error("Fejl på %s: %s", listing, e)
            continue

    # skriv aggregater (altid)
    write_custom_all(all_custom_elements, "data_all.xml")
    write_rss_all(all_events_flat, "out/rss-all.xml")

    logging.info("---- Resume ----")
    tot = 0
    for host, n in summary:
        logging.info("%s: %d events", host, n)
        tot += n
    logging.info("TOTAL: %d events", tot)
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv))
