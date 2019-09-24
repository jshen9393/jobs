from etl.indeed.indeed_extractor import get_parameters
from etl.indeed.indeed_extractor import IndeedAPI
from etl import constants
from etl import config

queries = get_parameters()

for query in queries:

    if query["zip"] is None:
        location = query["city"] + ", " + query["state"]
    else:
        location = query["zip"]

    params = {
        "v":constants.INDEED_API_VERSION,
        "format":constants.INDEED_FORMAT,
        "limit":constants.INDEED_LIMIT,
        "start":constants.INDEED_START,
        "highlight":constants.INDEED_HIGHLIGHT,
        "latlong":constants.INDEED_LATLONG,
        "publisher":config.INDEED_PUB_ID,
        "q":query["query"],
        "l":location,
        "sort":query["sort"],
        "radius":query["radius"],
        "fromage":query["fromage"],
        "st":query["site_type"],
        "jt":query["job_type"],
        "co":query["us"],
        "channel":query["channel"],
    }

    print (params)
#
# from etl.indeed.indeed_extractor import IndeedAPI as api
#
#
#
#
# results = api(**search)
#
# res = results.extract()
#
#
# c = conn.cursor()
#
# for result in res:
#     print(result)
#     insert_into(c,**result)
#
# c.close()