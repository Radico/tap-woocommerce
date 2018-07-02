from datetime import datetime

import singer

from tap_woocommerce.schemas import IDS

logger = singer.get_logger()


def metrics(tap_stream_id, records):
    with singer.metrics.record_counter(tap_stream_id) as counter:
        counter.increment(len(records))


def write_records(tap_stream_id, records):
    singer.write_records(tap_stream_id, records)
    metrics(tap_stream_id, records)


class BOOK(object):
    ORDERS = [IDS.ORDERS, 'date_modified']
    CUSTOMERS = [IDS.CUSTOMERS, 'date_modified']
    PRODUCTS = [IDS.PRODUCTS, 'date_modified']

    @classmethod
    def return_bookmark_path(cls, stream):
        return getattr(cls, stream.upper())

    @classmethod
    def get_incremental_syncs(cls):
        syncs = []
        for k, v in cls.__dict__.items():
            if not k.startswith("__") and not isinstance(v, classmethod):
                if len(v) > 1:
                    syncs.append(k)

        return syncs

    @classmethod
    def get_full_syncs(cls):
        syncs = []
        for k, v in cls.__dict__.items():
            if not k.startswith("__") and not isinstance(v, classmethod):
                if len(v) == 1:
                    syncs.append(k)

        return syncs


def sync(context):
    # do full syncs first as they are used later
    for stream in context.selected_stream_ids:
        if stream.upper() in BOOK.get_full_syncs():
            call_stream_full(context, stream)

    for stream in context.selected_stream_ids:
        if stream.upper() in BOOK.get_incremental_syncs():
            bk = call_stream_incremental(context, stream)
            save_state(context, stream, bk)


def call_stream_full(context, stream):
    offset = 1
    most_recent_record_date = None
    total_records = None

    while total_records is None or total_records >= 100:
        response = context.client.get(stream, offset)

        logger.info('{ts} - querying {stream} for {total}'
                    ' total results'.format(
                        ts=datetime.now(),
                        stream=stream,
                        total=response['total_records']
                    ))
        total_records = response['total_records']

        data = response['content']
        data = _clean_results(context, stream, data)
        write_records(stream, data)

        # max batch size of 100 via woocommerce API
        offset = offset + 100

    return most_recent_record_date


def call_stream_incremental(context, stream):
    context.update_start_date_bookmark(BOOK.return_bookmark_path(stream))

    offset = 1
    total_records = None

    while total_records is None or total_records >= 100:
        response = context.client.get(stream, offset)

        logger.info('{ts} - querying {stream} for {total}'
                    ' total results'.format(
                        ts=datetime.now(),
                        stream=stream,
                        total=response['total_records']
                    ))
        total_records = response['total_records']

        data = response['content']
        data = _clean_results(context, stream, data)
        write_records(stream, data)

        # max batch size of 100 via woocommerce API
        offset = offset + 100

    return context.max_date


def _clean_results(context, stream, data):
    clean_data = []
    last_updated = context.get_bookmark(BOOK.return_bookmark_path(stream))
    last_updated = convert_date(last_updated)

    for d in data:
        # dynamically get this/these path(s)?
        date = d.get('date_modified') or d.get('date_created')
        if not date:
            continue

        date = convert_date(date)
        if date is not None and date > last_updated:
            clean_data.append(d)
        if not context.max_date or date > context.max_date:
            context.max_date = date

    return clean_data


def convert_date(date):
    # date = date.replace('T', ' ') (do we need this?)
    return datetime.strptime(date, '%Y-%m-%dT%H:%M:%S')


def format_record_to_state_date(datestring):
    return datestring.replace(' ', 'T') + '+00:00'


def save_state(context, stream, bk):
    context.set_bookmark(BOOK.return_bookmark_path(stream), bk)
    context.write_state()
