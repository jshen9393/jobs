
'''
https://ads.indeed.com/jobroll/xmlfeed

'''
import json

import requests

from etl import constants
from etl.common.db import get_redshift
import psycopg2.extras


def get_parameters():
    with get_redshift() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute("""
            SELECT * FROM public.indeed_etl_jobs
            """
                           )
            for query in cursor.fetchall():
                yield query


class IndeedAPI:

    def __init__(self, **kwargs):
        self.params = kwargs

    def get_command(self):
        indeed_url = constants.INDEED_JOB_SEARCH
        r = requests.get(indeed_url, params=self.params)
        print (r.url)
        return json.loads(r.text)

    def extract(self):
        total_results = 1
        end = 0

        while total_results > end and end < 1025:
            results = self.get_command()
            total_results = results['totalResults']
            if total_results == 0:
                print('No Results')
                break
            if total_results > 1025:
                print('Too many results')
                break
            end = results['end']
            self.params['start'] = end + 1
            for result in results['results']:
                yield result

