# scrape_spielplan
Web scraper parsing table tennis results with own MySQL database class

```
urls = ['https://www.mytischtennis.de/clicktt/ByTTV/18-19/ligen/Bezirksoberliga/gruppe/323822/spielplan/gesamt',
        'https://www.mytischtennis.de/clicktt/WTTV/18-19/ligen/Sonderstaffel-BS/gruppe/334480/spielplan/gesamt']

g = ScrapeSpielplan()
g.scrape_spielplan(urls)
```

![Ergebnis Console](https://github.com/pkienle/scrape_spielplan/blob/master/result.jpg)

Example of inserted rows into database table:

![Ergebnis Datenbank](https://github.com/pkienle/scrape_spielplan/blob/master/result2.jpg)
