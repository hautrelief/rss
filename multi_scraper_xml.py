#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import sys
import time
import logging
from datetime import datetime
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from lxml import etree as ET  # real CDATA support

# ------------------------- Config / setup -------------------------

UA = "nemtilmeld-xml-multi/1.1 (+https://github.com/)"
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

S = requests.Session()
S.headers.update({
    "User-Agent": UA,
    "Accept-Language": "da-DK,da;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
})

# Danish month map + some short forms
MONTHS = {
    "jan": 1, "januar": 1,
    "feb": 2, "februar": 2,
    "mar": 3, "marts": 3,
    "apr": 4, "april": 4,
    "maj": 5,
    "jun": 6, "juni": 6,
    "jul": 7, "juli": 7,
    "aug": 8, "august": 8,
    "sep": 9, "sept": 9, "september": 9,
    "okt": 10, "oktober": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}

DATE_PATTERNS = [
    re.compile(r"(\d{1,2})\.?\s*([A-Za-zæøåÆØÅ]{3,10})\s*(\d{4})"),
    re.compile(r"(\d{1,2})\s*[-/.]\s*(\d{1,2})\s*[-/.]\s*(\d{2,4})"),
]
TIME_PATTERNS = [
    re.compile(r"kl\.?\s*(\d{1,2})[:.](\d{2})", re.I),
    re.compile(r"\b(\d{1,2})[:.](\d{2})\b"),
]
DEADLINE_PAT = re.compile(r"(deadline|tilmeldingsfrist)[:\s]*([\w .:-]+)", re.I)


# ------------------------- Helpers -------------------------

def site_root(u: str) -> str:
    """Normalize any NemTilmeld URL to the host root 'https://host/'."""
    p = urlparse(u.strip())
    if not p.scheme or not p.netloc:
        raise ValueError(f"Not a valid URL: {u}")
    return f"{p.scheme}://{p.netloc}/"


def fetch(url: str, retries: int = 3, timeout: int = 25) -> requests.Response:
    for attempt in range(retries):
        try:
            r = S.get(url, timeout=timeout)
            r.raise_for_status()
            return r
        except Exception as e:
            if attempt == retries - 1:
                raise
            time.sleep(2 ** attempt)


def clean_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def title_from_soup(soup: BeautifulSoup) -> str:
    h = soup.find("h1")
    if h and h.get_text(strip=True):
        return clean_spaces(h.get_text(" ", strip=True))
    og = soup.find("meta", property="og:title")
    if og and og.get("content"):
        return clean_spaces(og["content"])
    return "Arrangement"


def site_name_from_soup(soup: BeautifulSoup) -> str:
    ogs = soup.find("meta", property="og:site_name")
    if ogs and ogs.get("content"):
        return clean_spaces(ogs["content"])
    t = soup.find("title")
    if t and t.text:
        return clean_spaces(t.text)
    return ""


def desc_html(soup: BeautifulSoup) -> str:
    """Choose a reasonable description block, avoiding modals/alerts."""
    # Skip obvious modal/alert containers
    def bad(el):
        cls = " ".join(el.get("class", [])).lower()
        idv = (el.get("id") or "").lower()
        return ("modal" in cls or "alert" in cls or
                "modal" in idv or "alert" in idv)

    # Try common containers in priority order
    for sel in ["main", "article", "section", "div#content", "div.content", "div.container", "div.col", "div.row"]:
        el = soup.select_one(sel)
        if el and not bad(el):
            return str(el)

    # Fallback: take a handful of <p> paragraphs (skipping modal/alert)
    parts = []
    for p in soup.find_all("p"):
        if bad(p):
            continue
        parts.append(str(p))
        if len(parts) >= 8:
            break
    return "".join(parts)


def extract_dt(text: str):
    """Return (start_dt, end_dt) if we can parse a date/time."""
    text = text.replace("\xa0", " ")
    d = m = y = None
    for pat in DATE_PATTERNS:
        mo = pat.search(text)
        if mo:
            g = mo.groups()
            if not g[1].isdigit():
                d = int(g[0])
                m = MONTHS.get(g[1].lower().strip("."))  # textual month
                y = int(g[2])
                break
            else:
                d = int(g[0])
                m = int(g[1])
                y = int(g[2])
                if y < 100:
                    y += 2000 if y < 50 else 1900
                break

    hh = mm = None
    for tp in TIME_PATTERNS:
        to = tp.search(text)
        if to:
            hh = int(to.group(1))
            mm = int(to.group(2))
            break

    if d and m and y:
        if hh is None:
            hh = 9
        if mm is None:
            mm = 0
        st = datetime(y, m, d, hh, mm)
        en = datetime(y, m, d, min(23, hh + 2), mm)
        return st, en
    return None, None


def parse_times(soup: BeautifulSoup):
    txt = soup.get_text(" ", strip=True)
    st, en = extract_dt(txt)

    dl = None
    mo = DEADLINE_PAT.search(txt)
    if mo:
        dl, _ = extract_dt(mo.group(0))

    return st, en, dl


def parse_location(soup: BeautifulSoup, default_country="DK"):
    """Try to extract a location-ish block, very heuristic."""
    loc = {
        "type": "address",
        "name": "",
        "address": "",
        "zipcode": "",
        "city": "",
        "country": default_country,
    }

    # look for something that looks like "<street ...> <zip> <city>"
    txt = soup.get_text("\n", strip=True)
    mo = re.search(r"(.{5,120})\s+(\d{4})\s+([A-Za-zæøåÆØÅ .-]{2,80})", txt)
    if mo:
        loc["address"] = clean_spaces(mo.group(1))[:200]
        loc["zipcode"] = mo.group(2)
        loc["city"] = clean_spaces(mo.group(3))[:100]

    h = soup.find(["h2", "h3", "strong", "b"])
    if h and h.get_text(strip=True):
        loc["name"] = clean_spaces(h.get_text(" ", strip=True))[:120]

    return loc


IMG_SKIP_PAT = re.compile(
    r"(loading|creditcard_logo|nemtilmeld-logo|tracking|/assets/img/)",
    re.I
)


def parse_images(soup: BeautifulSoup, base_root: str):
    out, seen = [], set()
    for img in soup.find_all("img", src=True):
        src = img["src"]
        if src.startswith("data:"):
            continue
        absu = urljoin(base_root, src)
        if IMG_SKIP_PAT.search(absu):
            continue
        if absu in seen:
            continue
        seen.add(absu)
        out.append(absu)
    return out


def fmt_h(dt):
    if not dt:
        return ""
    try:
        # locale-independent 12h style (as in your sample)
        return dt.strftime("%Y-%m-%d %-I:%M %p")
    except ValueError:
        # Windows strftime has no %-I
        return dt.strftime("%Y-%m-%d %I:%M %p")


def fmt_c(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S") if dt else ""


def cdata_sub(parent, tag, val):
    el = ET.SubElement(parent, tag)
    el.text = ET.CDATA(val or "")
    return el


# ------------------------- Link discovery -------------------------

def event_links(listing_html: str, base_url: str):
    """
    Return absolute event URLs like 'https://host/<id>/' from
    any NemTilmeld listing page (handles /, /events/, etc.).
    """
    root = site_root(base_url)
    soup = BeautifulSoup(listing_html, "html.parser")
    out = set()

    for a in soup.find_all("a", href=True):
        try:
            absu = urljoin(root, a["href"])
            path = urlparse(absu).path.strip("/")
            if not path:
                continue
            segs = path.split("/")
            # accept first purely numeric segment as event id
            digit = next((seg for seg in segs if seg.isdigit()), None)
            if digit:
                out.add(urljoin(root, f"{digit}/"))
        except Exception:
            continue

    return sorted(out)


# ------------------------- XML assembly -------------------------

def build_provider(parent):
    prov = ET.SubElement(parent, "provider")
    cdata_sub(prov, "title", "NemTilmeld Aps")
    cdata_sub(prov, "address", "Strømmen 6")
    z = ET.SubElement(prov, "zipcode")
    z.text = ET.CDATA("9400")
    cdata_sub(prov, "city", "Nørresundby")
    cdata_sub(prov, "email", "info@nemtilmeld.dk")
    cdata_sub(prov, "phone", "+45 70404070")
    cdata_sub(prov, "website", "https://www.nemtilmeld.dk")
    return prov


def scrape_event(ev_url: str, base_root: str):
    r = fetch(ev_url)
    soup = BeautifulSoup(r.text, "html.parser")

    title = f"{title_from_soup(soup)}"
    site_name = site_name_from_soup(soup)
    if site_name and site_name not in title:
        # helpful context (as seen in your example)
        title = f"{title} | {site_name}"

    desc = desc_html(soup)
    imgs = parse_images(soup, base_root)
    st, en, dl = parse_times(soup)
    loc = parse_location(soup)

    # Extract org_event_id from URL path
    p = urlparse(ev_url).path.strip("/")
    org_id = (p.split("/")[0] if p else "")

    # Build <event>
    ev = ET.Element("event", id=org_id or "0")

    ET.SubElement(ev, "org_event_id").text = org_id or ""

    cdata_sub(ev, "title", title)
    d = ET.SubElement(ev, "description")
    d.text = ET.CDATA(desc or "")

    short = BeautifulSoup(desc or "", "html.parser").get_text(" ", strip=True)[:300]
    cdata_sub(ev, "description_short", short)

    # times
    ET.SubElement(ev, "start_time").text = fmt_h(st)
    ET.SubElement(ev, "end_time").text = fmt_h(en)
    ET.SubElement(ev, "deadline").text = fmt_h(dl)
    ET.SubElement(ev, "start_time_common").text = fmt_c(st)
    ET.SubElement(ev, "end_time_common").text = fmt_c(en)
    ET.SubElement(ev, "deadline_time_common").text = fmt_c(dl)

    # tickets placeholder
    ET.SubElement(ev, "tickets")
    ET.SubElement(ev, "available_tickets").text = "true"
    ET.SubElement(ev, "available_tickets_quantity")
    ET.SubElement(ev, "highest_ticket_price")
    ET.SubElement(ev, "few_tickets_left").text = "false"
    ET.SubElement(ev, "public_status").text = "registration_open"

    # url
    ET.SubElement(ev, "url").text = ev_url

    # images
    imgs_el = ET.SubElement(ev, "images")
    for i, src in enumerate(imgs):
        im = ET.SubElement(imgs_el, "image", id=str(i))
        ssrc = ET.SubElement(im, "source")
        ssrc.text = ET.CDATA(src)

    ET.SubElement(ev, "categories").text = " "

    # location
    le = ET.SubElement(ev, "location", id=org_id or "0")
    cdata_sub(le, "type", loc.get("type"))
    cdata_sub(le, "name", loc.get("name"))
    cdata_sub(le, "address", loc.get("address"))
    z2 = ET.SubElement(le, "zipcode")
    z2.text = ET.CDATA(loc.get("zipcode", ""))
    cdata_sub(le, "city", loc.get("city"))
    cdata_sub(le, "country", loc.get("country", "DK"))

    # organization (best-effort; fall back to host name)
    host = urlparse(base_root).netloc
    org = ET.SubElement(ev, "organization", id="0")
    cdata_sub(org, "title", site_name or host)
    cdata_sub(org, "address", "")
    city = loc.get("city") or ""
    cdata_sub(org, "city", city)
    cdata_sub(org, "phone", "")
    cdata_sub(org, "country", "DK")
    cdata_sub(org, "url", base_root)
    cdata_sub(org, "description", "")
    cdata_sub(org, "email", "")
    oz = ET.SubElement(org, "zipcode")
    oz.text = ET.CDATA(loc.get("zipcode", ""))

    # contact_details (placeholders)
    cd = ET.SubElement(ev, "contact_details")
    cdata_sub(cd, "name", site_name or host)
    cdata_sub(cd, "phone", "")
    cdata_sub(cd, "email", "")

    return ev


def scrape_site(base_url: str):
    """Return list[ET.Element] of <event> nodes for this site."""
    root = site_root(base_url)
    listing = fetch(root).text
    links = event_links(listing, root)
    logging.info("Found %d event link(s) on %s", len(links), root)

    events = []
    for url in links:
        try:
            ev = scrape_event(url, root)
            events.append(ev)
        except Exception as e:
            logging.warning("Failed %s: %s", url, e)

    return events


def write_site_xml(base_url: str, events, out_path: str):
    data = ET.Element("data")
    build_provider(data)
    evs = ET.SubElement(data, "events")
    for ev in events:
        evs.append(ev)

    xml_bytes = ET.tostring(
        data, encoding="utf-8", xml_declaration=True, pretty_print=True
    )
    with open(out_path, "wb") as f:
        f.write(xml_bytes)


def write_combined_xml(all_events, out_path: str):
    data = ET.Element("data")
    build_provider(data)
    evs = ET.SubElement(data, "events")
    for ev in all_events:
        evs.append(ev)

    xml_bytes = ET.tostring(
        data, encoding="utf-8", xml_declaration=True, pretty_print=True
    )
    with open(out_path, "wb") as f:
        f.write(xml_bytes)


# ------------------------- Main -------------------------

def main(argv):
    sources = argv[1:]
    if not sources:
        try:
            with open("sources.txt", "r", encoding="utf-8") as fh:
                sources = [
                    line.strip() for line in fh if line.strip() and not line.strip().startswith("#")
                ]
        except FileNotFoundError:
            print("No sources provided and sources.txt not found.")
            print("Usage: python multi_scraper_xml.py https://site1.nemtilmeld.dk/ https://site2.nemtilmeld.dk/")
            return 1

    # Normalize to host roots
    roots = []
    for s in sources:
        try:
            roots.append(site_root(s))
        except Exception as e:
            logging.warning("Skipping invalid source '%s': %s", s, e)

    if not roots:
        logging.error("No valid sources.")
        return 2

    # Ensure out dir exists
    import os
    os.makedirs("out", exist_ok=True)

    all_events = []
    per_site_counts = []

    for base in roots:
        try:
            logging.info("Scraping site: %s", base)
            events = scrape_site(base)
            host = urlparse(base).netloc.replace(":", "-")
            out_path = f"out/data-{host}.xml"
            write_site_xml(base, events, out_path)
            per_site_counts.append((host, len(events)))
            all_events.extend(events)
        except Exception as e:
            logging.error("Site failed %s: %s", base, e)
            per_site_counts.append((urlparse(base).netloc, 0))

    write_combined_xml(all_events, "data_all.xml")

    # Summary
    logging.info("---- Summary ----")
    total = 0
    for host, n in per_site_counts:
        logging.info("%s: %d event(s)", host, n)
        total += n
    logging.info("TOTAL events: %d", total)
    logging.info("Wrote data_all.xml and %d per-site file(s) in ./out/", len(per_site_counts))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
