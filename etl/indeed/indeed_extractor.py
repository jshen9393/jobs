
'''
https://ads.indeed.com/jobroll/xmlfeed

'''
import json

import requests
import psycopg2.extras

from etl.common.db import get_redshift
from etl import constants
from etl import config




class IndeedExtractor:

    def _get_command(self, query):
        indeed_url = constants.INDEED_JOB_SEARCH
        r = requests.get(indeed_url, params=query)
        return json.loads(r.text)

    def _get_parameters(self):

        with get_redshift() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute("""
                SELECT * FROM public.indeed_etl_jobs where is_active = TRUE 
                """
                               )
                for query in cursor.fetchall():
                    yield query

    def _query_mapping(self,params):

        if params["zip"] is None:
            location = params["city"] + ", " + params["state"]
        else:
            location = params["zip"]

        results = {
            "v": constants.INDEED_API_VERSION,
            "format": constants.INDEED_FORMAT,
            "limit": constants.INDEED_LIMIT,
            "start": constants.INDEED_START,
            "highlight": constants.INDEED_HIGHLIGHT,
            "latlong": constants.INDEED_LATLONG,
            "publisher": config.INDEED_PUB_ID,
            "q": params["query"],
            "l": location,
            "sort": params["sort"],
            "radius": params["radius"],
            "fromage": params["fromage"],
            "st": params["site_type"],
            "jt": params["job_type"],
            "co": params["country"],
            "channel": params["channel"],
        }

        return results

    def extract(self):
        for params in self._get_parameters():
            query = self._query_mapping(params)
            total_results = 1
            end = 0

            while total_results > end and end < 1025:
                results = self._get_command(query)
                total_results = results['totalResults']
                if total_results == 0:
                    print('No Results')
                    break
                if total_results > 1025:
                    print('Too many results')
                    continue
                end = results['end']
                query['start'] = end + 1
                for result in results['results']:
                    result['query'] = params['query']
                    yield result
