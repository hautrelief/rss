
# NemTilmeld XML feed – multi-site

Denne version kan scrape **flere** NemTilmeld-sites og generere:
- `data_all.xml` (alle events samlet i ét dokument)
- `out/data-<host>.xml` (ét dokument per site)

## Sådan gør du
1. Tilføj alle dine baser (én pr. linje) i `sources.txt`, fx:
   ```
   https://sclerose-bornholm.nemtilmeld.dk/
   https://sclerose-xxxx.nemtilmeld.dk/
   ```
2. Upload filerne i dette repo og aktiver GitHub Actions.
3. Workflowet kører hver 12. time og committer opdaterede XML-filer.
4. (Valgfrit) Slå GitHub Pages til for at få offentlige URLs til filerne.

## Kør lokalt
```bash
pip install -r requirements.txt
python multi_scraper_xml.py              # bruger sources.txt
# eller
python multi_scraper_xml.py https://site1.nemtilmeld.dk/ https://site2.nemtilmeld.dk/
```

> Skemaet følger dit eksempel (`<data><provider>...<events><event>...`) med CDATA for tekstfelter.
> Ticket- og kvoteoplysninger er placeholders; skriv hvis vi skal parse dem fra jeres sider.
