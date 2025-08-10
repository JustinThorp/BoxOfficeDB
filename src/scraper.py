import requests
from bs4 import BeautifulSoup
import sqlite3
from datetime import date, timedelta,datetime
import time

class Scraper:
    def __init__(self, date):
        self.date = date
        self.url = f"https://www.boxofficemojo.com/date/{date}/"

    def extract(self):
        response = requests.get(self.url)
        soup = BeautifulSoup(response.text, 'html.parser')
        rows = soup.find_all('tr')
        self.rows = rows[1:]
        text_rows = []
        for row in self.rows:
            temp = []
            for td in row.find_all('td'):
                link = td.find('a')
                if link:
                    temp.append(td.text)
                    temp.append(link.get('href'))
                else:
                    temp.append(td.text)
            text_rows.append(temp[:-2])
        self.text_rows = text_rows


    def transform(self):
        self.data = [[x.strip().replace(',', '').replace('$', '').replace('%', '') for x in row] for row in self.text_rows]
        for row in self.data:
            row[0] = int(row[0])
            row[1] = int(row[1]) if row[1] != '-' else None
            row[2] = row[2].strip()
            row[3] = row[3].split('/')[2]
            row[4] = int(row[4])
            row[5] = float(row[5].replace('<', '')) if row[5] != '-' else None
            row[6] = float(row[6].replace('<', '')) if row[6] != '-' else None
            row[7] = int(row[7]) if row[7] != '-' else None
            row[8] = int(row[8]) if row[8] != '-' else None
            row[9] = int(row[9]) if row[9] != '-' else None
            row[10] = int(row[10]) if row[10] != '-' else None
            row[11] = row[11].strip()
            if len(row) == 12:
                row.append(None)
            else:
                row[12] = row[12].split('/')[4]
            row.append(self.date)
            if row[10] is None:
                row.append(None)
            else:
                row.append(self.date - timedelta(days=row[10] - 1))

    def load(self):
        conn = sqlite3.connect('box_office.db')
        c = conn.cursor()
        c.executemany("INSERT OR REPLACE INTO box_office VALUES (?, ?, ?, ?, ?, ?, ?, ?,?,?,?,?,?,?,?)", self.data)
        conn.commit()
        conn.close()

    def run(self):
        self.extract()
        self.transform()
        self.load()

def daterange(start_date, end_date):
    days = int((end_date - start_date).days)
    for n in range(days):
        yield start_date + timedelta(n)

if __name__ == "__main__":
    start_date = date(2010, 1,1)
    end_date = date(2025, 8, 8)

    for scrapedate in daterange(start_date, end_date):
        print(scrapedate)
        time.sleep(1)
        scraper = Scraper(scrapedate)
        scraper.run()

    conn = sqlite3.connect('box_office.db')
    c = conn.cursor()
    c.execute("SELECT * FROM box_office LIMIT 10")
    rows = c.fetchall()
    for row in rows:
        print(row)
    conn.close()
