from datetime import datetime

from woocommerce import API

import singer

logger = singer.get_logger()


def _join(a, b):
    return a.rstrip("/") + "/" + b.lstrip("/")


class Client(object):
    def __init__(self, config):
        self.consumer_key = config.get('consumer_key')
        self.consumer_secret = config.get('consumer_secret')
        self.url = config.get('url')
        self.version = config.get('version')
        self.woocommerce_client = self._get_client()
        self.now_string = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def get(self, stream, offset):
        options = {
            'page': int((offset - 1) / 100 + 1),
            'per_page': 100,
            'orderby': 'date',
            'order': 'desc',
        }

        # get stream and order by date desc, offset = offset
        endpoint = self._build_request_string(stream, options)
        response = self.woocommerce_client.get(endpoint)
        import ipdb; ipdb.set_trace()

        response_json = response.json()
        total_records = len(response_json)
        latest_record = response_json[-1]

        response_dict = {
            'content': response_json,
            'total_records': total_records,
            'latest_record': latest_record
        }

        return response_dict

    def _get_client(self):
        return API(
            url=self.url,
            consumer_key=self.consumer_key,
            consumer_secret=self.consumer_secret,
            wp_api=True,
            version=self.version
        )

    @staticmethod
    def _build_request_string(stream, options):
        params = '?'

        for k, v in options.items():
            param = '{}={}&'.format(k, v)
            params += param

        endpoint = '{}{}'.format(stream, params.strip('&'))
        return endpoint
