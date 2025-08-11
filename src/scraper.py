import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import date, timedelta
import time
import os

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


    def transform(self) -> pd.DataFrame:
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
        columns = columns = ['rankTD','rankYD','title','TitleID','Daily','DayChange','WeekChange','theaters','avg','toDate','days','disttributor','distributorLink','date','ReleaseDate']
        output_df = pd.DataFrame(self.data,columns= columns)
        return output_df

        

    def run(self) -> pd.DataFrame:
        self.extract()
        return self.transform()

def daterange(start_date, end_date):
    days = int((end_date - start_date).days)
    for n in range(days):
        yield start_date + timedelta(n)

def clean_data(x: list[pd.DataFrame]) -> pd.DataFrame:
    df = pd.concat(x)
    df['rankTD'] = df['rankTD'].astype(pd.UInt16Dtype())
    df['rankYD'] = df['rankYD'].astype(pd.UInt16Dtype())
    df['Daily'] = df['Daily'].astype(pd.UInt32Dtype())
    df['DayChange'] = df['Daily'].astype(pd.Float32Dtype())
    df['WeekChange'] = df['WeekChange'].astype(pd.Float32Dtype())
    df['theaters'] = df['theaters'].astype(pd.UInt16Dtype())
    df['avg'] = df['avg'].astype(pd.UInt32Dtype())
    df['toDate'] = df['toDate'].astype(pd.UInt32Dtype())
    df['days'] = df['days'].astype(pd.UInt16Dtype())
    df['date'] = pd.to_datetime(df['date'])
    df['ReleaseDate'] = pd.to_datetime(df['ReleaseDate'])
    return df

if __name__ == "__main__":
    start_date = date(2025, 8,1)
    end_date = date(2025, 8, 8)
    data = []
    for scrapedate in daterange(start_date, end_date):
        print(scrapedate)
        time.sleep(1)
        scraper = Scraper(scrapedate)
        data.append(scraper.run())
    df= clean_data(data)
    if os.path.exists('box_office.parquet'):
        old_df = pd.read_parquet('box_office.parquet')
        old_df['key'] = old_df['TitleID'] + old_df['date'].astype(str)
        df['key'] = df['TitleID'] + df['date'].astype(str)
        old_df = old_df[~old_df['key'].isin(df['key'])]
        new_df = pd.concat([old_df,df]).sort_values(['date','rankTD'])
        new_df = new_df.drop('key',axis=1)
        new_df.to_parquet('box_office.parquet',index=False)
    else:
        print('gdfs')
        df.to_parquet('box_office.parquet',index=False)
