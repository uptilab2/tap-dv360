#!/usr/bin/env python3

import csv
from datetime import datetime, timedelta
from io import StringIO
import time

import backoff
from google.oauth2.credentials import Credentials
from googleapiclient import discovery, errors
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


@backoff.on_exception(backoff.expo,
                      errors.HttpError,
                      max_tries=10,
                      giveup=lambda e: 400 <= e.resp.status < 500)
@backoff.on_exception(backoff.expo, BrokenPipeError, max_tries=3)
def get_query(client, query_id):
    request = client.queries().getquery(queryId=query_id)
    return request.execute()


def discover(client):
    streams = []
    request = client.queries().listqueries()
    while request is not None:
        response = request.execute()
        LOGGER.info(response)
        for query_resource in response.get('queries', []):
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
        queries[stream.tap_stream_id] = get_query(client, stream.tap_stream_id)

        # Write schema
        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=stream.schema.to_dict(),
            key_properties=[],
        )

    # Trigger execution when needed
    now = datetime.now()
    running = set()

    for query_id, query_resource in queries.items():
        if query_resource['metadata']['latestReportRunTimeMs']:
            # skip recently executed
            last_run = datetime.fromtimestamp(
                int(query_resource['metadata']['latestReportRunTimeMs'][:-3])
            )
            if (now - last_run) < timedelta(minutes=int(config['rerun_threshold'] or 60)):
                LOGGER.info(f'Query {query_id} is was ran less than {config["rerun_threshold"]}min ago')
                continue

        if query_resource['metadata']['running']:
            # skip already running
            LOGGER.info(f'Query {query_id} is already running')
            running.add(query_id)
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
        queries[query_id] = get_query(client, query_id)

        # Check query is running
        if not queries[query_id]['metadata']['running']:
            raise Exception(f'Unable to run query {query_id}: {queries[query_id]}')
        running.add(query_id)

    # Wait for all queries to finish
    while running:
        for query_id in tuple(running):
            queries[query_id] = get_query(client, query_id)
            LOGGER.info(queries[query_id]['metadata'])
            if not queries[query_id]['metadata']['running']:
                running.remove(query_id)
        if running:
            LOGGER.info(f'Waiting for queries {running} to finish...')
            time.sleep(60)

    LOGGER.info('All queries completed !')
    for query_id, query_resource in queries.items():
        # Retrieve result csv from cloud storage signed url
        url = query_resource['metadata']['googleCloudStoragePathForLatestReport']
        LOGGER.info(f'Downloading report: {url}')
        response = requests.get(url)

        # Use csv reader to iterate on rows data
        reader = csv.reader(
            StringIO(response.content.decode()),
            delimiter=',',
            quotechar='"'
        )

        # Transform rows in singer-style dicts
        dimensions = query_resource['params']['groupBys']
        metrics = query_resource['params']['metrics']
        records = []
        for line_no, line in enumerate(reader):
            if line_no == 0:
                # Skip initial line
                continue

            # Map data to schema names
            record = {key: (value if value != '-' else 0) for (key, value) in zip(dimensions + metrics, line)}
            # No dimensions: sum line, stop here
            try:
                if all([not record.get(dim) for dim in dimensions]):
                    break
            except Exception:
                LOGGER.error(record)
                raise
            records.append(record)

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
