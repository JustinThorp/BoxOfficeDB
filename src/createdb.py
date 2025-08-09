import sqlite3
import os

if os.path.exists('box_office.db'):
    os.remove('box_office.db')

conn = sqlite3.connect('box_office.db')
c = conn.cursor()
c.execute('''CREATE TABLE IF NOT EXISTS box_office
             (rankTD INTEGER,rankYD INTEGER, title TEXT,TitleID TEXT,
             Daily INTEGER, DayChange REAL, WeekChange REAL,
             theaters INTEGER,avg REAL,
             toDate INTEGER, days INTEGER,disttributor TEXT,distributorLink TEXT,
             date TEXT,
             ReleaseDate TEXT,
             PRIMARY KEY (TitleID,date)) STRICT;''')
