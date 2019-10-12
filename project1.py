#!/usr/bin/env python3

import psycopg2
from datetime import datetime


DBNAME = "news"
q1 = "select articles.title , count(log.path) as views from articles" \
    " left join log on log.path like '%' || articles.slug where log.status " \
    "= '200 OK' group by articles.title order by views desc limit 3"
q2 = "select authors.name , t1.views from authors, (select articles.author" \
    " as author , count(log.path) as views from articles left join log on" \
    " log.path like '%' || articles.slug where log.status = '200 OK' group " \
    "by articles.author order by views desc) as t1 where t1.author = " \
    "authors.id"
q3 = "select t1.date, round((t2.errors::NUMERIC / t1.total) * 100 ,1) as " \
    "result from (select date_trunc('day', time) as date, count(1) as" \
    " total from log group by 1) as t1, (select date_trunc('day', time)" \
    " as date, count(1) as errors from log where log.status != '200 OK'" \
    " group by 1) as t2 where t1.date = t2.date and " \
    "round((t2.errors::NUMERIC / t1.total) * 100 ,1) > 1"

db = psycopg2.connect(database=DBNAME)
c = db.cursor()


print("\n1. What are the most popular three articles of all time?")
c.execute(q1)
for table in c.fetchall():
    print("  - \"{}\" - {} views".format(table[0], table[1]))


print("\n2. Who are the most popular article authors of all time?")
c.execute(q2)
for table in c.fetchall():
    print("  - {} - {} views".format(table[0], table[1]))


print("\n3. On which days did more than 1% of requests lead to errors?")
c.execute(q3)
for table in c.fetchall():
    date = table[0].strftime('%b %d,%Y')
    print("  - {} - {}% errors".format(date, table[1]))


print("")
db.close()
