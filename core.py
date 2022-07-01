import datetime
import requests
import json
import time
import pyodbc as db


class Application:
    counter = 0

    # [23.04.2022]: Constructor
    def __init__(self,
                 records_to_write,
                 database='BTC',
                 server='DESKTOP-VU19ML5',
                 driver='{ODBC Driver 17 for SQL Server}',
                 trust='yes',
                 log_path='C:/Users/bmaxl/Desktop/GitHub/Data-Scripts/btc-api/log.txt',
                 api='https://api.coindesk.com/v1/bpi/currentprice.json'
                 ):
        self.records_to_write = records_to_write
        self.database = database
        self.server = server
        self.driver = driver
        self.trust = trust
        self.log_path = log_path
        self.api = api

    # [30.03.22]: Setup MS SQL Connection
    def sql_server_connect(self):
        cursor = db.connect(f'DRIVER={self.driver};'
                            f'SERVER={self.server};'
                            f'DATABASE={self.database};'
                            f'Trusted_Connection={self.trust};').cursor()
        return cursor

    # [22.04.22]: Extract BTC record key
    def get_key_to_compare(self):
        p_key = self.sql_server_connect().execute(
            'SELECT TOP(2) local_time FROM dbo.BTC_rate ORDER BY local_time DESC;'
        ).fetchall()
        for i in p_key:
            for j in i:
                return str(j)[0:23]

    # [22.04.22]: Extract BTC record by key
    def get_record_to_compare(self, tgt_db='BTC'):
        last_record = self.get_key_to_compare()
        btc_usd = self.sql_server_connect().execute(
            f"SELECT TOP(1) BTC_USD FROM dbo.BTC_rate WHERE local_time = CAST('{last_record}' AS DATETIME);"
        ).fetchall()
        btc_eur = self.sql_server_connect().execute(
            f"SELECT TOP(1) BTC_EUR FROM dbo.BTC_rate WHERE local_time = CAST('{last_record}' AS DATETIME);"
        ).fetchall()
        btc_gbp = self.sql_server_connect().execute(
            f"SELECT TOP(1) BTC_GBP FROM dbo.BTC_rate WHERE local_time = CAST('{last_record}' AS DATETIME);"
        ).fetchall()
        rates = []
        for i in btc_usd:
            for usd_item in i:
                rates.append(float(usd_item))
        for j in btc_eur:
            for eur_item in j:
                rates.append(float(eur_item))
        for k in btc_gbp:
            for gbp_item in k:
                rates.append(float(gbp_item))
        return rates

    # [22.04.22]: Get diffs btn BTC old/BTC new
    def get_diff(self, old, new):
        zipped = zip(old, new)
        diff = []
        for old_i, new_i in zipped:
            diff.append(round(new_i - old_i, 6))
        return diff

    # [22.04.22]: Extract metrics
    def fetch_metrics(self):
        metrics = []
        retrieved = self.sql_server_connect().execute('SELECT * FROM fn_Metrics();').fetchall()
        for i in retrieved:
            for j in i:
                metrics.append(str(j))
        return metrics

    # [22.04.22]: Write log (console/log file)
    def write_log(self, log):
        with open(self.log_path, 'a') as file:
            file.write(log)
            file.close()

    # [22.04.2022]: Delete blank records
    def delete_blank(self):
        self.sql_server_connect().execute(
            'DELETE FROM dbo.BTC_rate WHERE local_time = '
            '(SELECT TOP(1) local_time FROM dbo.BTC_rate ORDER BY local_time ASC);'
        ).commit()

    # [22.04.2022]: Count records in tgt_table
    def count_records(self):
        count = [i[0] for i in self.sql_server_connect().execute('SELECT COUNT(*) FROM dbo.BTC_rate;')]
        return count[0]

    # [18.04.22]: Extracts BTC rates from an open API, Transforms them and Loads to the DB (each 60 secs)
    def get_btc_rate_by_time(self):
        while self.counter < self.records_to_write + 2:
            if self.counter == 0 and self.count_records() == 0:
                blank = f"INSERT INTO BTC_rate(BTC_USD, BTC_EUR, BTC_GBP)" \
                        f"VALUES(10.00, 10.00, 10.00);"
                self.sql_server_connect().execute(blank).commit()
            self.counter += 1
            json_data = json.loads(requests.get(self.api).text)
            utc_time = json_data['time']['updated']
            usd_rate = json_data['bpi']['USD']['rate'].replace(',', '')
            eur_rate = json_data['bpi']['EUR']['rate'].replace(',', '')
            gbp_rate = json_data['bpi']['GBP']['rate'].replace(',', '')
            olds = self.get_record_to_compare()
            news = [float(usd_rate), float(eur_rate), float(gbp_rate)]
            diffs = self.get_diff(olds, news)
            query = f"INSERT INTO BTC_rate(utc_time, BTC_USD, BTC_EUR, BTC_GBP, " \
                    f"BTC_USD_change, BTC_EUR_change, BTC_GBP_change) " \
                    f"VALUES('{utc_time}', {usd_rate}, {eur_rate}, {gbp_rate}," \
                    f"{diffs[0]}, {diffs[1]}, {diffs[2]});"
            self.sql_server_connect().execute(query).commit()
            metrics = self.fetch_metrics()
            log = f'\nDatetime: {datetime.datetime.now()}\n' \
                  f'USD: {diffs[0]} | EUR: {diffs[1]} | GBP: {diffs[2]}\n' \
                  f'Min rate: {metrics[1]} | Max rate: {metrics[2]} | Avg rate: {metrics[3]}\n' \
                  f'Min change: {metrics[4]} | Max change: {metrics[5]} | Avg change: {metrics[6]}\n' \
                  f'Std USD: {metrics[7]} | Std EUR: {metrics[8]} | Std GBP: {metrics[9]}\n'
            self.write_log(log=log)
            print(log)
            if self.count_records() <= 2:
                # (!) simplify
                if self.counter == 1:
                    self.delete_blank()
                elif self.counter == 2:
                    self.delete_blank()
            time.sleep(60)
        print('Successfully finished!')


