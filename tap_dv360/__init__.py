#!/usr/bin/env python3
import singer
from singer import utils
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema


REQUIRED_CONFIG_KEYS = [
    "refresh_token",
    "token_uri",
    "client_id",
    "client_secret",
    "start_date",
]
LOGGER = singer.get_logger()



def discover(client):
    streams = []
    response = client.queries().listqueries().execute()
    #TODO handle pagination with response['nextPageToken']
    for query_resource in response['queries']:
        schema = {
            'type': ['null', 'object'],
            'additionalProperties': False,
            'properties': {
                **{
                    key: {'type': ['null', 'string']}
                    for key in query_resource['params']['groupBys']
                },
                **{
                    key: {'type': ['null', 'number']}
                    for key in query_resource['params']['metrics']
                },
            }
        }
        metadata = [
            {
                'breadcrumb': [],
                'metadata': {'replication-method': 'FULL_TABLE'},
            },
        ]
        streams.append(
            CatalogEntry(
                tap_stream_id=query_resource['queryId'],
                stream=query_resource['metadata']['title'],
                schema=Schema.from_dict(schema),
                key_properties=[],
                metadata=metadata,
                replication_key=None,
                is_view=None,
                database=None,
                table=None,
                row_count=None,
                stream_alias=None,
            )
        )
    return Catalog(streams)


def sync(config, state, catalog):
    """ Sync data from tap source """
    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)

        bookmark_column = stream.replication_key
        is_sorted = True  # TODO: indicate whether data is sorted ascending on bookmark value

        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=stream.schema,
            key_properties=stream.key_properties,
        )

        # TODO: delete and replace this inline function with your own data retrieval process:
        tap_data = lambda: [{"id": x, "name": "row${x}"} for x in range(1000)]

        max_bookmark = None
        for row in tap_data():
            # TODO: place type conversions or transformations here

            # write one or more rows to the stream:
            singer.write_records(stream.tap_stream_id, [row])
            if bookmark_column:
                if is_sorted:
                    # update bookmark to latest value
                    singer.write_state({stream.tap_stream_id: row[bookmark_column]})
                else:
                    # if data unsorted, save max value until end of writes
                    max_bookmark = max(max_bookmark, row[bookmark_column])
        if bookmark_column and not is_sorted:
            singer.write_state({stream.tap_stream_id: max_bookmark})
    return


from google.oauth2.credentials import Credentials
from googleapiclient import discovery
API_NAME = 'doubleclickbidmanager'
API_VERSION = 'v1.1'
def get_client_from_config(config):
    credentials = Credentials(
        None,  # no access token here, generated from refresh token
        token_uri=config['token_uri'],
        refresh_token=config['refresh_token'],
        client_id=config['client_id'],
        client_secret=config['client_secret'],
    )
    return discovery.build(API_NAME, API_VERSION, credentials=credentials)



@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    client = get_client_from_config(args.config)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover(client)
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
