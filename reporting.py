from __future__ import division
import time
import sendgrid
import os
from sendgrid.helpers.mail import *
import datetime
from bittrex.tasks import mysqldb

from bittrex.tasks import fetch_markets

def report_for_market(market):
    count = mysqldb.extract("""select Count(*) from oneMinTick where Market='{0}'""".format(market))[0]['Count(*)']
    min_time = mysqldb.extract("""select Min(Timestamp) from oneMinTick where Market='{0}'""".format(market))[0]['Min(Timestamp)']
    max_time = mysqldb.extract("""select Max(Timestamp) from oneMinTick where Market='{0}'""".format(market))[0]['Max(Timestamp)']
    max_time = datetime.datetime.fromtimestamp(max_time)
    min_time = datetime.datetime.fromtimestamp(min_time)
    diff = (max_time - min_time).total_seconds()/60
    percent = (count/diff) * 100
    return {
        "market": market,
        "percentage": percent
        }

markets = fetch_markets()

list_of_percent = percent_of_market_data = map(report_for_market, markets)

def total_data_point_report():
    count = mysqldb.extract("""select Count(*) from oneMinTick""")[0]['Count(*)']
    min_time = float(mysqldb.extract("""select Min(Timestamp) from oneMinTick""")[0]['Min(Timestamp)'])
    max_time = float(mysqldb.extract("""select Max(Timestamp) from oneMinTick""")[0]['Max(Timestamp)'])
    max_time = datetime.datetime.fromtimestamp(max_time)
    min_time = datetime.datetime.fromtimestamp(min_time)
    diff = (max_time - min_time).total_seconds()/60
    percent = (count/(diff*289)) * 100
    return percent

total_percent_available = total_data_point_report()

def email_send():
    body_string = """
    Total Market: 289 \n
    Total Percent of Data : {0} \n
    """.format(total_percent_available)
    for x in list_of_percent:
        body_string = body_string + "{0} : {1}% \n".format(x['market'], x['percentage'])
    sg = sendgrid.SendGridAPIClient(apikey=os.environ.get('SENDGRID_API_KEY'))
    from_email = Email("nath.mitesh@gmail.com")
    to_email = Email("nath.mitesh@gmail.com")
    subject = "Data Quality Report"
    content = Content("text/plain", body_string)
    mail = Mail(from_email, subject, to_email, content)
    response = sg.client.mail.send.post(request_body=mail.get())
    return response.status_code
    
response = email_send()
print response
