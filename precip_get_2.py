import sqlite3
import urllib.parse, urllib.error, urllib.request
from urllib.request import urlopen
import ssl
import json

#Create table
conn = sqlite3.connect('precip_data.sqlite')
cur = conn.cursor()
cur.execute('''
            CREATE TABLE IF NOT EXISTS data
            (id INTEGER UNIQUE, date TEXT, station TEXT, station_name TEXT, precipitation  FLOAT,
            snow FLOAT, tmax FLOAT, tmin FLOAT, tavg FLOAT)
            ''')

#prevent duplicate entries
cur.execute('''
            CREATE UNIQUE INDEX IF NOT EXISTS ix_un ON data(date, station)
            ''')

#Retrieve daily summary data at specified station "US... "
stations = list()

baseurl = 'https://www.ncei.noaa.gov/access/services/data/v1?dataset=daily-summaries&options=includeStationName:true&format=json'
stations = '&stations=USW00014922,USW00014926,USW00014913,USW00014918,USW00014914,USW00014916,USW00014944,USW00014925,USW00014920'
# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE


start_year = 1901
end_year = 1902
errors = 0

while True:
    #Error debugging
    if start_year >= end_year:
        print('end_year must be greater than start_year')
        break
    if errors > 3:
        print('Maximum of 5 errors reached. Last search was', start_year-1, '-', month_day, 'to', end_year-1, '-', month_day)
        break

    #Build URL
    month_day = '-01-01'
    fullurl = baseurl + '&' + 'startDate=' + str(start_year) + month_day + '&' + 'endDate=' + str(end_year) + month_day + '&' + stations
    print('Retrieving:', fullurl)

    #deal with HTML errors
    try:
        connection  = urlopen(fullurl, context=ctx)
        data = connection.read().decode()

    except:
        print('Error retrieving records for', start_year, '-', month_day, 'to', end_year, '-', month_day )
        start_year = start_year + 1
        end_year = end_year +1
        errors = errors + 1
        time.sleep(3)
        continue

    js = json.loads(data)
    print(json.dumps(js, indent=4))

    #deal with data format errors
    if len(js) <1:
        print('No records found for', start_year, '-', month_day, 'to', end_year, '-', month_day)
        start_year = start_year + 1
        end_year = end_year + 1
        errors = errors + 1
        time.sleep(3)
        continue

    #Parse json data
    for report in js:
        #note missing parameters in database
        #tmax_exists = 1
        #tmin_exists = 1
        #tavg_exists = 1
        #snow_exists = 1

        report_date = report['DATE']
        report_station = report['STATION']
        station_name = report['NAME']
        if 'PRCP' in report:
            precip = report['PRCP']
        else:
            precip = None
        if 'SNOW' in report:
            snow = report['SNOW']
        else:
            snow = None
            #snow_exists = 0
        if 'TMAX' in report:
            tmax = report['TMAX']
        else:
            tmax = None
            #tmax_exists = 0
        if 'TMIN' in report:
            tmin = report['TMIN']
        else:
            tmin = None
            #tmin_exists = 0
        if 'TAVG' in report:
            tavg = report['TAVG']
        else:
            tavg = None
            #tavg_exists = 0


        #print('Report date:', report_date)
        #print('Station:', report_station)
        #print('Precipitation:', precip)
        #try:
            #print('Snow:', snow)
        #except:
            #continue
        #print(type(tmax))
        #print(type(snow))
        #print(type(station_name))
        #print(type(tmin))
        #print(type(tavg))

        cur.execute('''INSERT OR IGNORE INTO data (date, station, precipitation,
                    snow, station_name, tmax, tmin, tavg) VALUES (?,?,?,?,?,?,?,?)''',
                    (report_date, report_station, precip, snow, station_name, tmax,
                    tmin, tavg,))
    conn.commit()
    print('Retrieved data for', start_year, '-', month_day, 'to', end_year, '-', month_day)
    start_year = start_year + 1
    end_year = end_year + 1
    time.sleep(3)
    continue
