
import re, sys, time, logging
from datetime import datetime
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
from xml.dom import minidom
from pathlib import Path

UA="nemtilmeld-xml-multi/1.0 (+https://github.com/)"
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
S=requests.Session(); S.headers["User-Agent"]=UA

MONTHS={"jan":1,"januar":1,"feb":2,"februar":2,"mar":3,"marts":3,"apr":4,"april":4,"maj":5,"jun":6,"juni":6,"jul":7,"juli":7,"aug":8,"august":8,"sep":9,"sept":9,"september":9,"okt":10,"oktober":10,"nov":11,"november":11,"dec":12,"december":12}
DP=[re.compile(r"(\d{1,2})\.?\s*([A-Za-zæøåÆØÅ]{3,10})\s*(\d{4})"),
    re.compile(r"(\d{1,2})\s*[-/.]\s*(\d{1,2})\s*[-/.]\s*(\d{2,4})")]
TP=[re.compile(r"kl\.?\s*(\d{1,2})[:.](\d{2})",re.I),
    re.compile(r"\b(\d{1,2})[:.](\d{2})\b")]

def fetch(u,retries=3,timeout=20):
  for a in range(retries):
    try:
      r=S.get(u,timeout=timeout); r.raise_for_status(); return r
    except Exception:
      time.sleep(2**a)
  raise RuntimeError(f"fetch failed {u}")

def event_links(doc,base):
  s=BeautifulSoup(doc,"html.parser"); out=set()
  for a in s.find_all("a",href=True):
    u=urljoin(base,a["href"]); p=urlparse(u).path.strip("/")
    if p and p.split("/")[0].isdigit(): out.add(urljoin(base,p.split("/")[0]+"/"))
  return sorted(out)

def title(s):
  h=s.find("h1")
  if h and h.get_text(strip=True): return h.get_text(strip=True)
  og=s.find("meta",property="og:title")
  if og and og.get("content"): return og["content"].strip()
  return "Arrangement"

def desc_html(s):
  m=s.find("div",{"class":re.compile(r"(content|main|article)",re.I)})
  if m: return str(m)
  a=s.find("article")
  if a: return str(a)
  ps=s.find_all("p"); return "".join(str(p) for p in ps[:6])

def extract_dt(text):
  text=text.replace("\xa0"," "); d=m=y=None
  for pat in DP:
    mo=pat.search(text)
    if mo:
      g=mo.groups()
      if not g[1].isdigit():
        d=int(g[0]); m=MONTHS.get(g[1].lower().strip(".")); y=int(g[2]); break
      else:
        d=int(g[0]); m=int(g[1]); y=int(g[2]); 
        if y<100: y += 2000 if y<50 else 1900
        break
  hh=mm=None
  for tp in TP:
    to=tp.search(text)
    if to: hh=int(to.group(1)); mm=int(to.group(2)); break
  if d and m and y:
    if hh is None: hh=9
    if mm is None: mm=0
    st=datetime(y,m,d,hh,mm); en=datetime(y,m,d,min(23,hh+2),mm); return st,en
  return None,None

def parse_times(s):
  txt=s.get_text(" ",strip=True); st,en=extract_dt(txt)
  dl=None; mo=re.search(r"(deadline|tilmeldingsfrist)[:\s]*([\w .:-]+)",txt,re.I)
  if mo: dl,_=extract_dt(mo.group(0))
  return st,en,dl

def parse_loc(s, default_country="DK"):
  loc={"type":"address","name":"","address":"","zipcode":"","city":"","country":default_country}
  txt=s.get_text("\n",strip=True)
  mo=re.search(r"(.*)\s+(\d{4})\s+([A-Za-zæøåÆØÅ .-]+)",txt)
  if mo:
    loc["address"]=mo.group(1).strip()[:200]
    loc["zipcode"]=mo.group(2); loc["city"]=mo.group(3).strip()[:100]
  h=s.find(["h2","h3","strong","b"])
  if h: loc["name"]=h.get_text(strip=True)[:120]
  return loc

def parse_imgs(s, base):
  out=[]; seen=set()
  for img in s.find_all("img",src=True):
    src=img["src"]
    if src.startswith("data:"): continue
    if src in seen: continue
    seen.add(src); out.append(urljoin(base,src))
  return out

def ctext(p,tag,val):
  e=ET.SubElement(p,tag); e.text=f"<![CDATA[ {val or ''} ]]>"; return e

def fmt_h(dt):
  if not dt: return ""
  try: return dt.strftime("%Y-%m-%d %-I:%M %p")
  except: return dt.strftime("%Y-%m-%d %I:%M %p")

def fmt_c(dt): return dt.strftime("%Y-%m-%d %H:%M:%S") if dt else ""

def build_provider(parent):
  prov=ET.SubElement(parent,"provider")
  ctext(prov,"title","NemTilmeld Aps")
  ctext(prov,"address","Strømmen 6")
  z=ET.SubElement(prov,"zipcode"); z.text="<![CDATA[ 9400 ]]>"
  ctext(prov,"city","Nørresundby")
  ctext(prov,"email","info@nemtilmeld.dk")
  ctext(prov,"phone","+45 70404070")
  ctext(prov,"website","https://www.nemtilmeld.dk")
  return prov

def scrape_site(base_url):
  # returns list[ET.Element] of <event> nodes for this site
  events=[]
  links=event_links(fetch(base_url).text, base_url)
  for url in links:
    try:
      s=BeautifulSoup(fetch(url).text,"html.parser")
      ti=title(s); dh=desc_html(s); st,en,dl=parse_times(s); imgs=parse_imgs(s, base_url); loc=parse_loc(s)
      p=urlparse(url).path.strip("/"); org=(p.split("/")[0] if p else "")
      ev=ET.Element("event",attrib={"id":org or "0"})
      ET.SubElement(ev,"org_event_id").text=org or ""
      ctext(ev,"title",ti); d=ET.SubElement(ev,"description"); d.text=f"<![CDATA[ {dh} ]]>"
      short=BeautifulSoup(dh,"html.parser").get_text(" ",strip=True)[:300]; ctext(ev,"description_short",short)
      for tag,val in [("start_time",fmt_h(st)),("end_time",fmt_h(en)),("deadline",fmt_h(dl)),
                      ("start_time_common",fmt_c(st)),("end_time_common",fmt_c(en)),("deadline_time_common",fmt_c(dl))]:
        ET.SubElement(ev,tag).text=val
      ET.SubElement(ev,"tickets")
      ET.SubElement(ev,"available_tickets").text="true"
      ET.SubElement(ev,"available_tickets_quantity").text=""
      ET.SubElement(ev,"highest_ticket_price").text=""
      ET.SubElement(ev,"few_tickets_left").text="false"
      ET.SubElement(ev,"public_status").text="registration_open"
      ET.SubElement(ev,"url").text=url
      imgs_el=ET.SubElement(ev,"images")
      for i,src in enumerate(imgs):
        im=ET.SubElement(imgs_el,"image",attrib={"id":str(i)})
        ssrc=ET.SubElement(im,"source"); ssrc.text=f"<![CDATA[ {src} ]]>"
      ET.SubElement(ev,"categories").text=" "
      le=ET.SubElement(ev,"location",attrib={"id":org or '0'})
      ctext(le,"type",loc.get("type")); ctext(le,"name",loc.get("name")); ctext(le,"address",loc.get("address"))
      z2=ET.SubElement(le,"zipcode"); z2.text=f"<![CDATA[ {loc.get('zipcode')} ]]>"
      ctext(le,"city",loc.get("city")); ctext(le,"country",loc.get("country","DK"))
      orgn=ET.SubElement(ev,"organization",attrib={"id":"16571"})
      for k,v in [("title","Scleroseforeningens lokalafd. Bornholm"),("address","Kalbyvejen 13. Åkirkeby."),
                  ("city","Åkirkeby"),("phone","30450103"),("country","DK"),("url",base_url),
                  ("description",""),("email","frivillig@scleroseforeningen.dk")]:
        ctext(orgn,k,v)
      oz=ET.SubElement(orgn,"zipcode"); oz.text="<![CDATA[ 3720 ]]>"
      cd=ET.SubElement(ev,"contact_details")
      ctext(cd,"name","Scleroseforeningens organisationskonsulent, Scleroseforeningens lokalafd. Bornholm")
      ctext(cd,"phone","36463646"); ctext(cd,"email","frivillig@scleroseforeningen.dk")
      events.append(ev)
    except Exception as e:
      logging.warning("Failed %s: %s", url, e)
      continue
  return events

def write_site_xml(base_url, events, out_path):
  data=ET.Element("data")
  build_provider(data)
  evs=ET.SubElement(data,"events")
  for ev in events: evs.append(ev)
  xml=minidom.parseString(ET.tostring(data,encoding="utf-8")).toprettyxml(indent="  ",encoding="utf-8")
  Path(out_path).write_bytes(xml)

def write_combined_xml(all_events, out_path):
  data=ET.Element("data")
  build_provider(data)
  evs=ET.SubElement(data,"events")
  for ev in all_events: evs.append(ev)
  xml=minidom.parseString(ET.tostring(data,encoding="utf-8")).toprettyxml(indent="  ",encoding="utf-8")
  Path(out_path).write_bytes(xml)

def main(argv):
  # sources can be CLI args (bases) or sources.txt in current dir
  sources = argv[1:]
  if not sources:
    p=Path("sources.txt")
    if p.exists(): sources=[l.strip() for l in p.read_text(encoding="utf-8").splitlines() if l.strip() and not l.strip().startswith("#")]
    else: 
      print("No sources provided and sources.txt not found.")
      print("Usage: python multi_scraper_xml.py https://site1.nemtilmeld.dk/ https://site2.nemtilmeld.dk/ ...")
      sys.exit(1)
  out_dir=Path("out"); out_dir.mkdir(exist_ok=True, parents=True)

  all_events=[]
  for base in sources:
    base=base.strip()
    if not base.endswith("/"): base+="/"
    try:
      logging.info("Scraping %s", base)
      events=scrape_site(base)
      host=urlparse(base).netloc.replace(":","-")
      write_site_xml(base, events, out_dir / f"data-{host}.xml")
      all_events.extend(events)
    except Exception as e:
      logging.error("Site failed %s: %s", base, e)

  write_combined_xml(all_events, "data_all.xml")
  print(f"Wrote data_all.xml and {len(list(out_dir.glob('data-*.xml')))} per-site files.")

if __name__=="__main__":
  sys.exit(main(sys.argv))
