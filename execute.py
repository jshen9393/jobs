import sqlite3

from indeedapi import IndeedAPI as api
from sql import insert_into

search = {
    "publisher":"9551346561875460",
    "q":"python",
    "l":"charlotte nc",
    "fromage":"365",
    "start": "1",
    "v": "2",
    "format":"json"
}

results = api(**search)

res = results.extract()

conn = sqlite3.connect("jobs.db",isolation_level=None)

c = conn.cursor()

for result in res:
    print(result)
    insert_into(c,**result)

c.close()