from argparse import ArgumentParser
import json
import sys
import pandas
from time import time

from firestore import rules
from firestore_helpers import fetch_all, fetch_collection, fetch_collection_group
from edgedb_helpers import build_queries, run_bulk_inserts, run_bulk_resolved_queries, run_prereq_queries, run_bulk_queries
from utils import print_err, print_info, transform_source, trim_whitespace

parser = ArgumentParser()

parser.add_argument('-c', '--collection', action='store', help='The Firestore collection to fetch', required=True)
parser.add_argument(
  '-A', '--after',
  action='store',
  dest='start_after',
  help='Document reference to start after. Collection sort_by is defined in config source.',
  required=False,
)
parser.add_argument(
  '-l', '--limit',
  action='store',
  help='Overall limit for fetching from collection (debug)',
  type=int,
  default=-1
)
parser.add_argument('--column', action='store', dest='column', help='Fetch and transform a singular column (debug)')
parser.add_argument('-d', '--dry-run', action='store_true', help='Build queries but do not execute them')
parser.add_argument('--dump-invalid', action='store_true', help='Dump invalid rows to CSV')
parser.add_argument('--no-external', action='store_true', help='Do not call external services')
parser.add_argument('--no-transaction', action='store_true', help='Do not run queries in a transaction')
parser.add_argument('--bulk-insert', action='store_true', help='Use bulk INSERT and a for..in..union statement instead of individual INSERTs')

args = parser.parse_args(sys.argv[1:])

def run_task(
  collection_name,
  dry_run=False,
  dump_invalid=False,
  no_external=False,
  no_transaction=False,
  bulk_insert=False,
  column=None,
  limit=-1,
  start_after=None,
):
  if collection_name not in rules:
    print_err(f'No transformation rules for collection {collection_name}')
    return

  meta = rules[collection_name]
  order_by = meta['fetch_order']
  mapping = meta['mapping']
  group_by = meta.get('group_by', None)
  is_col_group = meta.get('is_collection_group', False)
  table_name = meta.get('edgedb_table_name', None)
  query_suffix = meta.get('edgedb_query_suffix', '')
  iterated_query = meta.get('edgedb_iterated_query', None)
  row_resolver_function = meta.get('row_resolver_function', None)
  row_resolvers = meta.get('edgedb_row_resolvers', {})
  skip_row_if_empty = meta.get('skip_row_if_empty', [])
  edgedb_type_casts = meta.get('edgedb_type_casts', {})
  prerequisites = meta.get('edgedb_prereq_queries', [])

  DEBUG_single_column = column is not None

  fetch_start = time()
  docs = []
  if is_col_group:
    print_info(f'Fetching {collection_name} as a collection group...')
    if limit >= 0:
      docs = fetch_collection_group(collection_name, limit, order_by, start_after).get('result', [])
    else:
      docs = fetch_collection_group(collection_name, None, order_by, start_after).get('result', [])
  else:
    if limit >= 0:
      docs = fetch_collection(collection_name, limit, order_by, start_after).get('result', [])
    else:
      docs = fetch_all(collection_name, order_by, start_after)
  source_df = pandas.DataFrame(docs)
  fetch_end = time()
  print('Loaded columns: ', list(source_df.columns))
  print(f'Fetch time: {fetch_end - fetch_start}s')

  # if update_nonce:
  #   print(f'Updating nonce for {len(source_df.index)} items in {collection_name}...')
  #   current_nonce = int(time())
  #   source_df['update_nonce'] = current_nonce
  #   if not dry_run:
  #     if update_nonce and limit < 0:
  #       print_err('Will not call update_nonce without a limit, as this would update all documents in the collection!')
  #       return
  #     batch_update_nonce(source_df, collection_name, 'update_nonce', current_nonce)
  #   return

  transform_start = time()
  if DEBUG_single_column:
    if not column in source_df.columns:
      print_err(f'Column {column} not found in collection {collection_name} (specified with --column)')
      return
    if not dry_run:
      print_err('Cannot fetch a single column without --dry-run since generated queries would insert partial data.')
      return
    output = transform_source(source_df, mapping, group_by, no_external, column)
  else:
    output = transform_source(source_df, mapping, group_by, no_external)
    output = run_prereq_queries(output, prerequisites)
  transform_end = time()
  print(f'Transform time: {transform_end - transform_start}s')

  query_start = time()
  if bulk_insert:
    print(f'Query: {iterated_query} (with bulk insert)')
    if not dry_run:
      print(f'will run on {len(output)} rows')
      run_bulk_inserts(output, iterated_query)
  elif row_resolver_function:
    print(f'Query: {row_resolver_function} (with row resolvers)')
    if not dry_run:
      print(f'will run on {len(output)} rows')
      run_bulk_resolved_queries(output, row_resolver_function, row_resolvers)
  else:
    querybuilder_start = time()
    built_queries = build_queries(
      output,
      table_name,
      edgedb_type_casts,
      query_suffix,
      skip_row_if_empty,
      dump_invalid,
    )
    querybuilder_end = time()
    print(f'Query builder time: {querybuilder_end - querybuilder_start}s')

    print('\nQueries (head):')
    print(trim_whitespace(json.dumps(list(built_queries[:5]))))
    print('\nQueries (tail):')
    print(trim_whitespace(json.dumps(list(built_queries[-5:]))))
    if not dry_run:
      run_bulk_queries(built_queries, no_transaction)
  query_end = time()
  print(f'Query time: {query_end - query_start}s')


task_start = time()
run_task(
  args.collection,
  args.dry_run,
  args.dump_invalid,
  args.no_external,
  args.no_transaction,
  args.bulk_insert,
  args.column,
  args.limit,
  args.start_after,
)
task_end = time()
print(f'Task time: {task_end - task_start}s')

