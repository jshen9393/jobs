
'''
https://ads.indeed.com/jobroll/xmlfeed

'''
import json

import requests
import psycopg2.extras

from etl.common.db import get_postgres
from etl import constants
from etl import config


class IndeedExtractor:

    def _get_command(self, query):
        indeed_url = constants.INDEED_JOB_SEARCH
        r = requests.get(indeed_url, params=query)
        return json.loads(r.text)

    def _get_parameters(self):

        with get_postgres() as conn:
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


class IndeedDuration:

    def _get_command(self, query):
        indeed_url = constants.INDEED_GET_JOB
        r = requests.get(indeed_url, params=query)
        return json.loads(r.text)

    def _get_open_jobs(self):

        with get_postgres() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute("""
                select distinct job_key 
                from public.indeed_jobs_duration
                    where job_expired = '1900-01-01'
                    and job_no_api = '1900-01-01'
                """
                               )
                for query in cursor.fetchall():
                    yield query

    def _query_mapping(self, job_key):

        query = {
            "v": constants.INDEED_API_VERSION,
            "format": constants.INDEED_FORMAT,
            "jobkeys": job_key,
            "publisher": config.INDEED_PUB_ID,
        }

        return query

    def extract(self):
        for job_key in self._get_open_jobs():
            query = self._query_mapping(job_key['job_key'])
            result = self._get_command(query)
            api_active = True
            try:
                expired_status = result['results'][0]['expired']
            except IndexError:
                api_active = False
                expired_status = False

            yield job_key['job_key'], expired_status, api_active
