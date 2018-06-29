#!/usr/bin/env python3
import singer

from singer import utils
from singer.catalog import Catalog, CatalogEntry, Schema

from tap_woocommerce.context import Context
from tap_woocommerce import streams as streams_, schemas


REQUIRED_CONFIG_KEYS = [
    "consumer_key",
    "consumer_secret",
    "url",
    "version",
    "start_date",
]
LOGGER = singer.get_logger()


def check_credentials_are_authorized(ctx):
    pass


def discover(ctx):
    check_credentials_are_authorized(ctx)
    catalog = Catalog([])
    for tap_stream_id in schemas.stream_ids:
        schema = Schema.from_dict(schemas.load_schema(tap_stream_id),
                                  inclusion="automatic")
        catalog.streams.append(CatalogEntry(
            stream=tap_stream_id,
            tap_stream_id=tap_stream_id,
            key_properties=schemas.PK_FIELDS[tap_stream_id],
            schema=schema,
        ))
    return catalog


def sync(ctx):
    for tap_stream_id in ctx.selected_stream_ids:
        schemas.load_and_write_schema(tap_stream_id)
    streams_.sync(ctx)
    ctx.write_state()


@utils.handle_top_exception(LOGGER)
def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    ctx = Context(args.config, args.state)
    if args.discover:
        discover(ctx).dump()
        print()
    else:
        import ipdb
        ipdb.set_trace()
        ctx.catalog = args.catalog
        sync(ctx)


if __name__ == "__main__":
    main()
