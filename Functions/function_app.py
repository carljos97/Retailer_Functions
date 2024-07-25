import logging
import azure.functions as func
import pyodbc
from datetime import datetime, timezone, timedelta, date
import sys
import os
import struct

app = func.FunctionApp()

today = None
first = None
lastMonth = None
current_month_partial = None
current_month = None
measurement_date = None

"""
Database connection
"""

def db_connect():

    driver = '{ODBC Driver 17 for SQL Server}'
    server = 'tcc-comerc.database.windows.net,1433'
    database = 'tcc-comerc-bd'
    username = 'carlosjos'
    password = 'insert-password'
    # Create connection object
    cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';ENCRYPT=yes;UID='+username+';PWD='+ password+';TrustServerCertificate=no')

    return cnxn.cursor()

def check_consumption():
    try:
        cursor = db_connect()
        cursor.execute("SELECT AVG(date_measurement) FROM consumer_unit_measurements WHERE consumer_unit_id = 1 AND date = '{}'%".format(measurement_date))
        row = cursor.fetchone()
        cursor.commit()
        measurement_current_month = row[0]
        cursor.execute("SELECT {} FROM contracts_volume WHERE consumer_unit_id = 1")
        row_1 = cursor.fetchone()
        cursor.commit()
        contract_volume = row_1[0]
        if measurement_current_month > contract_volume:
            description = 'Sugere-se diminuir a carga para a UC. Limite contratual próximo.'
            cursor.execute("INSERT INTO consumer_unit_dispatch_suggestion (date_suggestion, suggestion, consumer_unit_id) VALUES ('{}', '{}', 1)".format(measurement_date, description))
            cursor.commit()
        cursor.close()
               
    except:
        logging.info('Houve um problema ao checar os dados de medição.')
        pass

@app.schedule(schedule="'0 0 9 * *'", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def tcc_suggestions(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    today = datetime.date.today()
    first = today.replace(day=1)
    lastMonth = first - datetime.timedelta(days=1)
    current_month_partial = lastMonth.replace(day=1)
    current_month = str(current_month_partial)
    measurement_date = str(current_month_partial.strftime("%Y-%m"))

    check_consumption()

    logging.info('Python timer trigger function executed.')