#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Bygger feeds pr. entitet KUN fra HTML:

- out/rss-<host>.xml      (RSS 2.0 – strukturelt som NemTilmelds gamle feed)
- out/data-<host>.xml     (jeres custom XML: <data><provider/><events>…)
- out/rss-all.xml         (samlet RSS 2.0)
- data_all.xml            (samlet custom XML)

Input-kilder i sources.txt:
  - Kan være https://<host>/, https://<host>/events/ eller gamle feed-URL’er
    (https://<host>/events/list/feed). Vi normaliserer selv til en HTML-
    liste-side (/events/ hvis muligt, ellers forsiden).
"""

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

# ------------------------- config -------------------------

UA = "sclerose-nemtilmeld-html/2.0 (+github actions)"
S = requests.Session()
S.headers.update({
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "da-DK,da;q=0.9,en;q=0.6"
})

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# DK dato/tid
MONTHS = {
    "jan":1, "januar":1,
    "feb":2, "februar":2,
    "mar":3, "marts":3,
    "apr":4, "april":4,
    "maj":5,
    "jun":6, "juni":6,
    "jul":7, "juli":7,
    "aug":8, "august":8,
    "sep":9, "sept":9, "september":9,
    "okt":10, "oktober":10,
    "nov":11, "november":11,
    "dec":12, "december":12
}
DATE_PATS = [
    re.compile(r"(\d{1,2})\.?\s*([A-Za-zæøåÆØÅ]{3,12})\s*(\d{2,4})"),
    re.compile(r"(\d{1,2})\s*[-/.]\s*(\d{1,2})\s*[-/.]\s*(\d{2,4})"),
]
TIME_PATS = [
    re.compile(r"kl\.?\s*(\d{1,2})[:.](\d{2})", re.I),
    re.compile(r"\b(\d{1,2})[:.](\d{2})\b")
]

# ------------------------- tiny utils -------------------------

def fetch(url: str, retries: int = 3, timeout: int = 30, allow_redirects: bool = True) -> requests.Response:
    for i in range(retries):
        try:
            r = S.get(url, timeout=timeout, allow_redirects=allow_redirects)
            r.raise_for_status()
            return r
        except Exception as e:
            if i == retries-1:
                raise
            time.sleep(2**i)

def host_of(u: str) -> str:
    return urlparse(u).netloc

def root_of(u: str) -> str:
    p = urlparse(u)
    return f"{p.scheme}://{p.netloc}/"

def to_listing_url(src: str) -> str:
    """Normaliser enhver kilde-URL til en HTML-liste-side."""
    src = src.strip()
    if not re.match(r"^https?://", src):
        raise ValueError(f"Ugyldig URL: {src}")
    # Hvis det er den gamle feed-URL → /events/
    if "/events/list/feed" in src:
        return root_of(src) + "events/"
    # Hvis det er /events/ allerede
    if "/events" in urlparse(src).path:
        return src if src.endswith("/") else src + "/"
    # Ellers forsøg /events/, fald tilbage til forsiden
    base = root_of(src)
    try:
        r = fetch(urljoin(base, "events/"))
        if r.ok:
            return urljoin(base, "events/")
    except Exception:
        pass
    return base

def norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").replace("\xa0", " ")).strip()

def parse_dk_datetime(text: str):
    """Heuristik: find (start,end) i dansk brødtekst."""
    if not text:
        return None, None
    t = norm_space(text.lower())

    # dato
    d = m = y = None
    for pat in DATE_PATS:
        mo = pat.search(t)
        if mo:
            g = mo.groups()
            if not g[1].isdigit():  # '2. september 2025'
                d = int(g[0])
                m = MONTHS.get(g[1].strip(".").lower(), None)
                y = int(g[2])
            else:                   # '02-09-2025'
                d = int(g[0]); m = int(g[1]); y = int(g[2])
                if y < 100: y += 2000 if y < 50 else 1900
            break

    # tid
    hh = mm = None
    for tp in TIME_PATS:
        mt = tp.search(t)
        if mt:
            hh = int(mt.group(1)); mm = int(mt.group(2))
            break

    if d and m and y:
        if hh is None: hh = 9
        if mm is None: mm = 0
        start = datetime(y, m, d, hh, mm)
        end = datetime(y, m, d, min(23, hh+2), mm)
        return start, end
    return None, None

def fmt_h(dt: datetime | None) -> str:
    if not dt: return ""
    try:
        return dt.strftime("%Y-%m-%d %-I:%M %p")
    except Exception:
        return dt.strftime("%Y-%m-%d %I:%M %p")

def fmt_c(dt: datetime | None) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S") if dt else ""

def make_cdata(parent, tag, text=""):
    el = ET.SubElement(parent, tag)
    el.text = ET.CDATA(text or "")
    return el

def safe_abs(base: str, href: str) -> str:
    try:
        return urljoin(base, href)
    except Exception:
        return href

def looks_like_event_link(abs_url: str, host: str) -> bool:
    """Interne event-sider har path /123/."""
    p = urlparse(abs_url)
    return (p.netloc == host) and bool(re.search(r"/\d{1,8}/?$", p.path))

# ------------------------- scraping -------------------------

def find_site_title(soup: BeautifulSoup, host: str) -> str:
    # prøv: <title>, brand-header, logo alt
    if soup.title and soup.title.text.strip():
        return norm_space(soup.title.text)
    logo = soup.select_one('img[alt*="Sclerose"], img[alt*="logo"], img[alt*="Logo"]')
    if logo and logo.get("alt"):
        return norm_space(logo["alt"])
    return host

def event_blocks_from_listing(soup: BeautifulSoup):
    """
    Returner kandidat-"blokke" der ligner events (div/li/article).
    Gør det bredt – vi deduplikerer senere på link.
    """
    blocks = set()
    # typiske wrappers
    for sel in [
        "div.list-item", "div.event", "li.event", "article", "div.media", "div.card", "li", "div"
    ]:
        for el in soup.select(sel):
            # blokker med 'Læs mere' eller et tydeligt link
            if el.select_one('a[href*="facebook."], a[href*="/"]'):
                blocks.add(el)
    return list(blocks)

def extract_event_from_block(block, base: str, host: str):
    """
    Udtræk (title, link, teaser, start, end, is_internal).
    """
    # link prioritet: intern event -> ekstern
    a_best = None
    intern = None
    extern = None
    for a in block.select("a[href]"):
        href = a.get("href", "")
        absu = safe_abs(base, href)
        if looks_like_event_link(absu, host):
            intern = a
            break
        if urlparse(absu).netloc and urlparse(absu).netloc != host:
            # kandidér ekstern
            extern = extern or a
        elif "/events/" not in absu and absu.endswith("/"):
            # måske intern root-link – lad stå
            extern = extern or a
    a_best = intern or extern

    link = safe_abs(base, a_best["href"]) if a_best else ""

    # titel
    title = ""
    for hsel in ["h1","h2","h3","h4",".title",".event-title","strong"]:
        el = block.select_one(hsel)
        if el and norm_space(el.get_text()):
            title = norm_space(el.get_text())
            break
    if not title and a_best:
        title = norm_space(a_best.get_text())

    # teaser/description kort
    teaser = ""
    p = block.find("p")
    if p and norm_space(p.get_text()):
        teaser = norm_space(p.get_text())
    # fallback: kort tekst af hele blokken
    if not teaser:
        teaser = norm_space(block.get_text())
        teaser = " ".join(teaser.split()[:80])

    # tider
    start, end = parse_dk_datetime(" ".join([
        title, teaser, norm_space(block.get_text())
    ]))

    return {
        "title": title,
        "link": link,
        "teaser": teaser,
        "start": start,
        "end": end,
        "is_internal": bool(intern and looks_like_event_link(link, host))
    }

def scrape_listing(listing_url: str):
    """Hent liste-siden og udtræk kandidater."""
    r = fetch(listing_url)
    soup = BeautifulSoup(r.text, "html.parser")
    host = host_of(listing_url)
    site_title = find_site_title(soup, host)
    base = root_of(listing_url)

    # find blokke → udtræk events
    events = []
    seen_links = set()
    for b in event_blocks_from_listing(soup):
        ev = extract_event_from_block(b, base, host)
        if not ev["title"] and not ev["link"]:
            continue
        # deduplikér på link (ellers på (title,start))
        key = ev["link"] or (ev["title"], ev["start"])
        if key in seen_links: 
            continue
        seen_links.add(key)
        events.append(ev)

    # sortér på start
    events.sort(key=lambda e: e["start"] or datetime.max)
    return site_title, events

def augment_from_detail(ev: dict, host: str):
    """
    Hvis ev er intern, prøv at hente detaljer – uden at følge eksterne redirects.
    Hvis siden svarer 3xx til ekstern host, behold ekstern link og teaser.
    """
    if not ev["is_internal"] or not ev["link"]:
        return ev

    try:
        r = fetch(ev["link"], allow_redirects=False)
        # ekstern redirect?
        if 300 <= r.status_code < 400:
            loc = r.headers.get("Location", "")
            if loc and urlparse(loc).netloc and urlparse(loc).netloc != host:
                ev["link"] = loc  # peg direkte på ekstern side
                ev["is_internal"] = False
                return ev

        soup = BeautifulSoup(r.text, "html.parser")
        # titel (mere præcis)
        if soup.find("h1"):
            t = norm_space(soup.find("h1").get_text())
            if t: ev["title"] = t

        # beskrivelse – tag hovedindhold (robust, bred)
        main = soup.select_one("#content, main, .content, .container, .col-content")
        if not main: main = soup
        # fjern navigation/footers
        for bad in main.select("nav, header, footer, script, style"):
            bad.decompose()

        html = str(main)
        # forkortet tekst til RSS summary
        plain = norm_space(main.get_text())
        teaser = " ".join(plain.split()[:120])
        if teaser:
            ev["teaser"] = teaser

        # prøv igen at udlede tider fra detail
        st, en = parse_dk_datetime(plain)
        if st and not ev["start"]: ev["start"] = st
        if en and not ev["end"]:   ev["end"] = en

        # gem hele html’en som 'detail_html' (til custom XML description)
        ev["detail_html"] = html

    except Exception as e:
        # stiltiende – behold liste-data
        pass

    return ev

# ------------------------- writers -------------------------

def provider_xml(parent):
    prov = ET.SubElement(parent, "provider")
    make_cdata(prov, "title", "NemTilmeld Aps")
    make_cdata(prov, "address", "Strømmen 6")
    z = ET.SubElement(prov, "zipcode"); z.text = ET.CDATA("9400")
    make_cdata(prov, "city", "Nørresundby")
    make_cdata(prov, "email", "info@nemtilmeld.dk")
    make_cdata(prov, "phone", "+45 70404070")
    make_cdata(prov, "website", "https://www.nemtilmeld.dk")
    return prov

def build_custom_event(ev: dict, host: str, site_title: str):
    # id: prøv at udlede fra link (/123/). Ellers en simpel hash.
    m = re.search(r"/(\d{1,8})/?$", urlparse(ev["link"]).path or "")
    ev_id = m.group(1) if m else str(abs(hash((host, ev["title"], ev["link"]))) % 10**9)

    el = ET.Element("event", id=ev_id)
    ET.SubElement(el, "org_event_id").text = ev_id
    make_cdata(el, "title", ev["title"] or "Arrangement")

    desc_html = ev.get("detail_html")
    if not desc_html:
        # lav en enkel HTML af teaser + link
        t = ev["teaser"] or ""
        if ev.get("link"):
            t += f'<p><a href="{ev["link"]}" target="_blank" rel="noopener">Læs mere</a></p>'
        desc_html = f"<div>{t}</div>"
    d = ET.SubElement(el, "description"); d.text = ET.CDATA(desc_html)

    short = (ev["teaser"] or "")[:300]
    make_cdata(el, "description_short", short)

    ET.SubElement(el, "start_time").text = fmt_h(ev.get("start"))
    ET.SubElement(el, "end_time").text   = fmt_h(ev.get("end"))
    ET.SubElement(el, "deadline")

    ET.SubElement(el, "start_time_common").text = fmt_c(ev.get("start"))
    ET.SubElement(el, "end_time_common").text   = fmt_c(ev.get("end"))
    ET.SubElement(el, "deadline_time_common")

    # tickets/availability – ukendt fra HTML-listen → defaults
    ET.SubElement(el, "tickets")
    ET.SubElement(el, "available_tickets").text = "true"
    ET.SubElement(el, "available_tickets_quantity")
    ET.SubElement(el, "highest_ticket_price")
    ET.SubElement(el, "few_tickets_left").text = "false"
    ET.SubElement(el, "public_status").text = "registration_open"

    ET.SubElement(el, "url").text = ev.get("link") or ""
    ET.SubElement(el, "images")    # tom
    ET.SubElement(el, "categories").text = " "

    loc = ET.SubElement(el, "location", id=ev_id)
    make_cdata(loc, "type", "address"); make_cdata(loc, "name", "")
    make_cdata(loc, "address", ""); z = ET.SubElement(loc, "zipcode"); z.text = ET.CDATA("")
    make_cdata(loc, "city", ""); make_cdata(loc, "country", "DK")

    org = ET.SubElement(el, "organization", id="0")
    make_cdata(org, "title", site_title or host)
    make_cdata(org, "address", ""); make_cdata(org, "city", "")
    make_cdata(org, "phone", "");  make_cdata(org, "country", "DK")
    make_cdata(org, "url", f"https://{host}/")
    make_cdata(org, "description", ""); make_cdata(org, "email", "")
    oz = ET.SubElement(org, "zipcode"); oz.text = ET.CDATA("")

    cd = ET.SubElement(el, "contact_details")
    make_cdata(cd, "name", site_title or host); make_cdata(cd, "phone", ""); make_cdata(cd, "email", "")

    return el

def write_custom_for_site(host: str, site_title: str, events: list, out_path: str):
    data = ET.Element("data")
    provider_xml(data)
    evs = ET.SubElement(data, "events")
    for ev in events:
        evs.append(build_custom_event(ev, host, site_title))
    xml = ET.tostring(data, encoding="utf-8", xml_declaration=True, pretty_print=True)
    with open(out_path, "wb") as f: f.write(xml)

def write_custom_all(all_event_elements: list, out_path: str):
    data = ET.Element("data")
    provider_xml(data)
    evs = ET.SubElement(data, "events")
    for el in all_event_elements:
        evs.append(el)
    xml = ET.tostring(data, encoding="utf-8", xml_declaration=True, pretty_print=True)
    with open(out_path, "wb") as f: f.write(xml)

def write_rss_for_site(host: str, site_title: str, site_url: str, events: list, out_path: str):
    rss = ET.Element("rss", version="2.0")
    ch = ET.SubElement(rss, "channel")
    ET.SubElement(ch, "title").text = site_title or host
    ET.SubElement(ch, "link").text  = site_url
    ET.SubElement(ch, "description").text = f"Arrangementer fra {site_title or host}"
    ET.SubElement(ch, "language").text = "da-DK"
    now = datetime.now(timezone.utc)
    ET.SubElement(ch, "pubDate").text = now.strftime("%a, %d %b %Y %H:%M:%S %z")

    # sortér på start
    events_sorted = sorted(events, key=lambda e: e.get("start") or now)

    for ev in events_sorted:
        it = ET.SubElement(ch, "item")
        ET.SubElement(it, "title").text = ev["title"] or "Arrangement"
        ET.SubElement(it, "link").text  = ev.get("link") or ""
        g = ET.SubElement(it, "guid", isPermaLink="true"); g.text = ev.get("link") or ""
        desc = ET.SubElement(it, "description")
        # brug detail_html hvis vi har, ellers teaser
        html = ev.get("detail_html") or f"<div>{ev.get('teaser','')}</div>"
        desc.text = ET.CDATA(html)
        # pubDate = start eller nu
        dt = ev.get("start") or now
        ET.SubElement(it, "pubDate").text = dt.strftime("%a, %d %b %Y %H:%M:%S %z")

    xml = ET.tostring(rss, encoding="utf-8", xml_declaration=True, pretty_print=True)
    with open(out_path, "wb") as f: f.write(xml)

def write_rss_all(all_events: list, out_path: str):
    rss = ET.Element("rss", version="2.0")
    ch = ET.SubElement(rss, "channel")
    ET.SubElement(ch, "title").text = "Scleroseforeningen – samlede arrangementer"
    ET.SubElement(ch, "link").text  = "https://scleroseforeningen.dk/"
    ET.SubElement(ch, "description").text = "Aggregat af lokale arrangementer (HTML-scrapet)"
    ET.SubElement(ch, "language").text = "da-DK"
    now = datetime.now(timezone.utc)
    ET.SubElement(ch, "pubDate").text = now.strftime("%a, %d %b %Y %H:%M:%S %z")

    events_sorted = sorted(all_events, key=lambda e: e.get("start") or now)
    for ev in events_sorted:
        it = ET.SubElement(ch, "item")
        ET.SubElement(it, "title").text = ev["title"] or "Arrangement"
        ET.SubElement(it, "link").text  = ev.get("link") or ""
        g = ET.SubElement(it, "guid", isPermaLink="true"); g.text = ev.get("link") or ""
        desc = ET.SubElement(it, "description")
        html = ev.get("detail_html") or f"<div>{ev.get('teaser','')}</div>"
        desc.text = ET.CDATA(html)
        dt = ev.get("start") or now
        ET.SubElement(it, "pubDate").text = dt.strftime("%a, %d %b %Y %H:%M:%S %z")

    xml = ET.tostring(rss, encoding="utf-8", xml_declaration=True, pretty_print=True)
    with open(out_path, "wb") as f: f.write(xml)

# ------------------------- main -------------------------

def load_sources(argv: list[str]) -> list[str]:
    if argv:
        return [a.strip() for a in argv if a.strip()]
    urls = []
    try:
        with open("sources.txt", "r", encoding="utf-8") as fh:
            for line in fh:
                s = line.strip()
                if not s or s.startswith("#"): 
                    continue
                urls.append(s)
    except FileNotFoundError:
        logging.error("sources.txt ikke fundet – angiv URL’er som argumenter eller tilføj filen.")
        sys.exit(2)
    return urls

def main(argv):
    srcs = load_sources(argv[1:])
    listing_urls = []
    for s in srcs:
        try:
            listing_urls.append(to_listing_url(s))
        except Exception as e:
            logging.warning("Springer %s (%s)", s, e)

    if not listing_urls:
        logging.error("Ingen gyldige kilder.")
        return 3

    os.makedirs("out", exist_ok=True)

    all_custom_elements = []
    all_events_flat = []
    summary = []

    for listing in listing_urls:
        host = host_of(listing)
        base = root_of(listing)

        logging.info("Scraper: %s", listing)
        site_title, events = scrape_listing(listing)

        # suppler fra detail for interne links (uden at følge eksterne redirects)
        enriched = []
        for ev in events:
            enriched.append(augment_from_detail(ev, host))

        # skriv per-site filer
        custom_path = f"out/data-{host}.xml"
        write_custom_for_site(host, site_title, enriched, custom_path)

        rss_path = f"out/rss-{host}.xml"
        write_rss_for_site(host, site_title, base, enriched, rss_path)

        # til samlede filer
        for ev in enriched:
            all_events_flat.append(ev)
            all_custom_elements.append(build_custom_event(ev, host, site_title))

        summary.append((host, len(enriched)))

    # Combined
    write_custom_all(all_custom_elements, "data_all.xml")
    write_rss_all(all_events_flat, "out/rss-all.xml")

    logging.info("---- Resume ----")
    tot = 0
    for host, n in summary:
        logging.info("%s: %d events", host, n); tot += n
    logging.info("TOTAL: %d events", tot)
    logging.info("Skrevet: data_all.xml, out/rss-all.xml samt per-site out/*.xml")
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv))
