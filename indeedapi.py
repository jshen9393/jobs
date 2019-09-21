import requests

'''
https://ads.indeed.com/jobroll/xmlfeed

'''
import json

import requests


class IndeedAPI:

    def __init__(self, **kwargs):
        self.params = kwargs
        self.params['format'] = 'json'
        self.params['v'] = '2'

    def get_command(self):
        indeed_url = 'http://api.indeed.com/ads/apisearch'
        r = requests.get(indeed_url, params=self.params)
        print (r.url)
        return json.loads(r.text)

    def extract(self):
        total_results = 1
        end = 0

        while total_results > end:
            results = self.get_command()
            total_results = results['totalResults']
            if total_results == 0:
                print('No Results')
                break
            end = results['end']
            self.params['start'] = end + 1
            for result in results['results']:
                yield result

