import requests
from bs4 import BeautifulSoup
import pymysql
from MySQL import MySQL


class ScrapeSpielplan(MySQL):

    def __init__(self):
        #self.url = url
        self.spielplan_table = None
        self.spielplan_rows = None
        self.spielplan_len = None

    def _get_sp_header(self, url):
        """ Create requests Header and parse URL for correct referer URL """

        referer = url.replace('/gesamt', '/vr')
        headers =  {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate', 'Accept-Language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',
            'Cache-Control': 'max-age=0', 'Connection': 'keep-alive', 'Referer': referer,
            'Host': 'www.mytischtennis.de',
            'User-Agent':
            'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36'}
        
        return headers

    def _parse_bemerkungen(self, cell):
        """ Extract Bemerkungen, z.B. verlegt (v) oder Heimrechttausch (t)
        Schwierigkeit besteht darin, nur den Content zu parsen, der auch Inhalt hat
        """

        status = []
        for item in cell:
            try:
                if len(item.text) < 4:
                    status.append(item.text)
            except Exception:
                pass
        result = ";".join(item.strip() for item in status)
        return result

    def _get_page(self, url):
        print("Connecting...")
        headers = self._get_sp_header(url)
        connection = requests.get(url, headers=headers)
        if connection.status_code != 200:
            return False

        html = connection.content
        soup = BeautifulSoup(html, 'html.parser')
        return soup

    def find_spielplan_table(self, url):
        try:
            soup = self._get_page(url)
            spielplan_table = soup.find('table', {'id': 'playingPlanDesktop'})
            spielplan_table = spielplan_table.find('tbody')
            self.spielplan_table = spielplan_table
        except Exception:
            pass

    def find_spielplan_rows(self):
        try:
            self.spielplan_rows = self.spielplan_table.findAll('tr')
            self.spielplan_len = len(self.spielplan_rows)
            #print(self.spielplan_len)
        except Exception:
            pass

    def get_spielplan_row(self, i):
        return self.spielplan_rows[i]

    def loop_spielplan_rows(self, url):
        print(self.spielplan_len)
        collect_results = []
        collect_urlspielberichte = []

        try:
            for i in range(self.spielplan_len):
                result = ['' for _ in range(14)]
                row = self.get_spielplan_row(i)

                datum, i_new = '', 0
                while len(datum) == 0:
                    row_i = self.get_spielplan_row(i-i_new)
                    datum = row_i.findAll('td')[0].text.strip().replace('\n', '')
                    i_new += 1

                result[0] = datum.split(' ')[0].strip()
                result[1] = ' '.join(el.strip() for el in datum.split(' ')[1:])
                uhrzeit = row.findAll('td')[1].contents
                result[2] = uhrzeit[0].string.strip()

                # if len(uhrzeit) > 1:
                #result[3] = uhrzeit[1].string.strip()
                # else:
                #result[3] = None

                result[3] = self._parse_bemerkungen(row.findAll('td')[1])

                heim_url = row.findAll('td')[3].find('a')
                auswaerts_url = row.findAll('td')[4].find('a')

                if heim_url is not None:
                    result[4] = 'https://www.mytischtennis.de' + heim_url['href']
                if auswaerts_url is not None:
                    result[5] = 'https://www.mytischtennis.de' + auswaerts_url['href']

                result[6] = row.findAll('td')[3].text.strip()
                result[7] = row.findAll('td')[4].text.strip()

                url_ergebnis = row.findAll('td')[6].findAll('a')
                if len(url_ergebnis) >= 1:
                    result[8] = url_ergebnis[0].text.strip()
                    url_spielbericht = 'https://www.mytischtennis.de' + url_ergebnis[0]['href'].strip()

                if len(url_ergebnis) == 1:
                    result[9] = ''

                if len(url_ergebnis) > 1:
                    result[9] = ";".join(item.text.strip() for item in url_ergebnis[1:])

                groupid = url_spielbericht[url_spielbericht.lower().find('/gruppe/')+8:].split('/')[0].strip()
                meetingid = url_spielbericht[url_spielbericht.lower().find('/spielbericht/')+14:].split('/')[0].strip()

                result[10] = meetingid
                result[11] = groupid

                if len(url_spielbericht) > 0:
                    collect_results.append((result[0], result[1], result[2], result[3], result[4], result[5], result[6],
                                            result[7], result[8], result[9], result[10], result[11]))
                    collect_urlspielberichte.append((url_spielbericht, url, result[10], result[11]))

                url_ergebnis, url_spielbericht, groupid, meetingid = '', '', '', ''
        except Exception:
            pass

        return collect_results, collect_urlspielberichte
    
    def insert_spielplan_results(self, results):
        """ Insert Spielplan Results into MySQL table """

        sql = "INSERT INTO scrape_groupresults (wochentag, datum_temp, uhrzeit, note, home_url, away_url, \
                home, away, result, status, meetingid, groupid) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        
        self.execute_many(sql, results)

    def insert_urls_spielberichte(self, urls):
        """ Insert Spielplan Results into MySQL table """

        sql = "INSERT INTO scrape_urlspielberichte_new (URL, referer_URL, meetingid, groupid, updated) VALUES (%s, %s, %s, %s, now())"

        self.execute_many(sql, urls)


    def scrape_spielplan(self, urls):
        for url in urls:
            self.find_spielplan_table(url)
            self.find_spielplan_rows()

            for attempt in range(1, 6):
                print("Attempt #{}\n{}".format(attempt,url))
                results, urls_spielberichte = self.loop_spielplan_rows(url)

                #print(results, "\n", spielberichte)
                if results or urls_spielberichte:
                    break

            self.insert_spielplan_results(results)
            self.insert_urls_spielberichte(urls_spielberichte)
            print("\n")


urls = ['https://www.mytischtennis.de/clicktt/ByTTV/18-19/ligen/Bezirksoberliga/gruppe/323822/spielplan/gesamt',
        'https://www.mytischtennis.de/clicktt/WTTV/18-19/ligen/Sonderstaffel-BS/gruppe/334480/spielplan/gesamt']

g = ScrapeSpielplan()
#g = ScrapeSpielplan('https://www.google.de')
g.scrape_spielplan(urls)

