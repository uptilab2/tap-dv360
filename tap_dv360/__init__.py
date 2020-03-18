#!/usr/bin/env python3

import csv
from datetime import datetime, timedelta
from io import BytesIO
import time

from google.oauth2.credentials import Credentials
from googleapiclient import discovery
import requests
import singer
from singer import utils
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema

LOGGER = singer.get_logger()

API_NAME = 'doubleclickbidmanager'
API_VERSION = 'v1.1'

REQUIRED_CONFIG_KEYS = [
    "refresh_token",
    "token_uri",
    "client_id",
    "client_secret",
    "rerun_threshold",
]


def build_schema(query_resource):
    return Schema.from_dict({
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
    })


def discover(client):
    streams = []
    request = client.queries().listqueries()
    while request is not None:
        response = request.execute()
        for query_resource in response['queries']:
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
                    schema=build_schema(query_resource),
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
        request = client.queries().listqueries_next(request, response)
    return Catalog(streams)


def sync(client, config, catalog):
    """ Sync data from tap source """
    # Loop over selected streams in catalog
    queries = {}

    # Get current status for each query
    for stream in catalog.get_selected_streams({}):
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)
        request = client.queries().getquery(queryId=stream.tap_stream_id)
        queries[stream.tap_stream_id] = request.execute()

        # Write schema
        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=stream.schema.to_dict(),
            key_properties=[],
        )

    # Trigger execution when needed
    now = datetime.now()
    for query_id, query_resource in queries.items():
        if query_resource['metadata']['running']:
            # skip already running
            LOGGER.info(f'Query {query_id} is already running')
            continue

        if query_resource['metadata']['latestReportRunTimeMs']:
            # skip recently executed
            last_run = datetime.fromtimestamp(
                int(query_resource['metadata']['latestReportRunTimeMs'][:-3])
            )
            if (now - last_run) < timedelta(minutes=int(config['rerun_threshold'] or 60)):
                LOGGER.info(f'Query {query_id} is was ran less than 1h ago')
                continue

        # Trigger the others
        schedule = query_resource.get('schedule')
        request = client.queries().runquery(queryId=query_id, body={
            'dataRange': 'ALL_TIME',
            'timezoneCode': schedule['nextRunTimezoneCode'] if schedule else 'Europe/Paris',
        })
        LOGGER.info(f'Running query {query_id}')
        request.execute()

        # Refresh query status
        request = client.queries().getquery(queryId=query_id)
        queries[query_id] = request.execute()

        # Check query is running
        if not queries[query_id]['metadata']['running']:
            raise Exception(f'Unable to run query {query_id}: {queries[query_id]}')

    # Wait for all queries to finish
    running = set(queries)
    iteration = 0
    while running:
        for query_id in running:
            request = client.queries().getquery(queryId=query_id)
            queries[query_id] = request.execute()
            LOGGER.info(queries[query_id]['metadata'])
        running = {query_id for query_id, query_resource in queries.items() if query_resource['metadata']['running']}
        if running:
            LOGGER.info(f'Waiting for queries {running} to finish...')
            # sleep 10 sec the first 2 minutes, then 1 minute. rocket science.
            iteration += 1
            time.sleep(10 if iteration < 12 else 60)

    LOGGER.info('All queries completed !')
    for query_id, query_resource in queries.items():
        url = query_resource['metadata']['googleCloudStoragePathForLatestReport']
        response = requests.get(url)
        reader = csv.reader(BytesIO(response.body), delimiter=',', quotechar='"')
        records = []
        for line_no, line in enumerate(reader):
            # Skip initial line
            if line_no == 0:
                continue
            keys = query_resource['params']['groupBys'] + query_resource['params']['metrics']

            # Map data to schema names
            records.append({key: value for (key, value) in zip(keys, line)})

        # Write records to stdout
        singer.write_records(str(query_id), records)


def get_client_from_config(config):
    credentials = Credentials(
        None,  # no access token here, generated from refresh token
        token_uri=config['token_uri'],
        refresh_token=config['refresh_token'],
        client_id=config['client_id'],
        client_secret=config['client_secret'],
    )
    return discovery.build(
        API_NAME,
        API_VERSION,
        credentials=credentials,
        cache_discovery=False,  # to avoid warnings
    )


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
        sync(client, args.config, args.catalog)


if __name__ == "__main__":
    main()
