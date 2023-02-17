import json
import os
from typing import Callable
from edgedb import create_client
from google.api_core import datetime_helpers
import pandas

from utils import datetime_to_rfc3339, print_err, print_info, print_success, print_warn, row_to_csv, str_escape, is_null

client = create_client(
  dsn=os.environ['EDGEDB_DSN'],
  tls_security='insecure',
)


def wrap_expression(expr, edgedb_cast):
  output = ''
  if edgedb_cast:
    output += f'<{edgedb_cast}>'

  if type(expr) == str:
    output += f'"{str_escape(expr)}"'
  else:
    output += f'{expr}'
  return output


def should_skip(metadata):
  if '__prereq_valid' in metadata and metadata['__prereq_valid'] == False:
    return True
  if 'isDeleted' in metadata and bool(metadata['isDeleted']):
    return True
  if 'testingAccount' in metadata and bool(metadata['testingAccount']):
    return True
  return False


def row_id(row):
  if 'firebase_uid' in row:
    return row['firebase_uid']
  if 'firebase_id' in row:
    return row['firebase_id']
  return None


def get_query_builder(
  transformed_df: pandas.DataFrame,
  edgedb_collection: str,
  type_casts: dict,
  query_suffix: str,
  skip_row_if_empty: list,
):
  # Any scalar field not present in type_casts will be "quoted" and escaped, but not cast to <str>.
  # Any dict- or list-like field will be cast to <json>.
  columns = transformed_df.columns
  def query_builder(row):
    id = row_id(row)
    vars = {}
    query = f'insert {edgedb_collection} {{'
    stop_triggered = False

    for field in columns:
      expr = row[field]
      subquery = None
      edgedb_cast = type_casts[field] if field in type_casts else None
      if (field == 'metadata'):
        if type(expr) == dict and should_skip(expr):
          print_warn(f'query: Skipping row {id} due to metadata: {json.dumps(expr)}')
          stop_triggered = True
          break
        else:
          continue

      if is_null(expr):
        if field in skip_row_if_empty:
          print_warn(f'query: Skipping row {id} due to empty field {field}')
          stop_triggered = True
          break
        else:
          continue

      if type(expr) == list:
        subqueries = []
        for e in expr:
          if type(e) == dict:
            if '__query' in e:
              if '__vars' in e:
                if all(is_null(v) for v in e['__vars'].values()):
                  continue
                subqueries.append({
                  '__query': e['__query'],
                  '__vars': e['__vars'],
                })
              else:
                raise Exception('Validation: __vars must be specified when using __query.')

      if type(expr) == str:
        expr = expr
      if type(expr) == dict:
        if '__query' in expr:
          if '__vars' in expr:
            if all(is_null(v) for v in expr['__vars'].values()):
              continue
            vars.update(expr['__vars'])
            subquery = expr['__query']
          else:
            raise Exception('Validation: __vars must be specified when using __query.')
        elif len(expr.keys()) > 0:
          expr = json.dumps(expr)
          edgedb_cast = 'json'
        else:
          continue
      if type(expr) == pandas.Timestamp or type(expr) == datetime_helpers.DatetimeWithNanoseconds:
        expr = datetime_to_rfc3339(expr)
      if subquery:
        query += f' {field} {subquery},'
      else:
        query += f' {field} := {wrap_expression(expr, edgedb_cast)},'

    if stop_triggered:
      return { '__valid': False, '__row': row_to_csv(row) }

    query += '} ' + query_suffix
    query_info = { '__valid': True, '__q': query, '__v': vars }
    # print(query_info)
    return query_info

  return query_builder


def row_validator(row):
  return '__valid' in row and row['__valid'] == True


def unnest_row(series):
  return series.apply(lambda r: r.get('__row', None))


def remap_vars(vars, row):
  result = {}
  for k, v in vars.items():
    if type(v) == str:
      if type(row[v]) == dict and '__sourceValue' in row[v]:
        result[k] = row[v]['__sourceValue']
      elif type(row[v]) == str:
        result[k] = row[v]
      else:
        result[k] = None
  return result


def run_prereq_queries(df: pandas.DataFrame, prereq_queries: list):
  def run_query_on_row(query, vars, row):
    if 'metadata' in row and row['metadata'].get('__prereq_valid', True) == False:
      print_info(f'Skipping row {row_id(row)} due to metadata: {json.dumps(row["metadata"])}')
      return row

    print_info(f'Running prereq query: {query}')
    metadata = {}
    v = remap_vars(vars, row)
    try:
      res = client.query(query, **v)
      valid = res[0] > 0
      metadata['__prereq_valid'] = valid
      if not valid:
        print_warn(f'Prereq query failed, vars: {json.dumps(v)}')
    except Exception as e:
      print_err(f"Error executing query '{query}' with variables {json.dumps(v)}")
      print_err(f"Error: {e}")
      metadata['__prereq_valid'] = False
    row['metadata'] = metadata
    return row
  
  for query in prereq_queries:
    q = query['query']
    v = query['vars']
    df = df.apply(lambda r: run_query_on_row(q, v, r), axis='columns')

  return df


def build_queries(
  transformed_df: pandas.DataFrame,
  edgedb_collection: str,
  type_casts: dict,
  query_suffix: str,
  skip_row_if_empty: list = [],
  dump_invalid: bool = False,
):
  builder = get_query_builder(
    transformed_df,
    edgedb_collection,
    type_casts,
    query_suffix,
    skip_row_if_empty,
  )
  built_all = transformed_df.apply(builder, axis='columns')
  built_non_na = built_all.dropna()
  loc_valid = built_non_na.apply(row_validator)
  built_valid = built_non_na.loc[loc_valid]
  built_invalid = built_non_na.loc[loc_valid.apply(lambda x: not x)]

  if dump_invalid and built_invalid.shape[0] > 0:
    filename = f'{edgedb_collection}_invalid.csv'
    unnest_row(built_invalid).to_csv(filename, index=False)
    print_info(f'--dump-invalid: rows dumped to {filename}')


  print(f'Processed {len(built_all)} rows.')
  print(f'Skipped {len(built_all) - len(built_non_na)} rows which were NA.')
  print(f'Skipped {len(built_invalid)} rows which did not pass validation checks.')
  print_success(f'Built {len(built_valid)} valid queries.')
  return built_valid


def run_bulk_inserts_base(
  source_df: pandas.DataFrame = None,
  iterated_query: str = None,
):
  json_data = source_df.to_json(orient='records')
  try:
    client.query(iterated_query, data=json_data)
  except Exception as e:
    print_err(f"Error executing query '{iterated_query}' with {json_data}")
    print_err(f"Exception: {e}")
  return


def run_bulk_inserts(
  source_df: pandas.DataFrame = None,
  iterated_query: str = None,
):
  if type(source_df) == pandas.core.groupby.DataFrameGroupBy:
    print_info(f'Running bulk inserts for grouped dataframe')
    for group_name, group_df in source_df:
      print_info(f'Running bulk inserts for group {group_name}')
      run_bulk_inserts(group_df, iterated_query)
  else:
    run_bulk_inserts_base(source_df, iterated_query)


def run_bulk_resolved_queries(
  source_df: pandas.DataFrame = None,
  row_resolver_function: Callable = None,
  row_resolvers: dict = {}
):
  for i, row in source_df.iterrows():
    print_info(f'Running bulk resolved query for row {i}')
    run_bulk_resolved_query(row.to_dict(), row_resolver_function, row_resolvers)


def run_bulk_resolved_query(
  row: dict = None,
  row_resolver_function: Callable = None,
  row_resolvers: dict = {}
):
  json_data = json.dumps(row)
  row_resolver = None
  try:
    resolution = row_resolver_function(row)
    if resolution not in row_resolvers:
      print_err(f'Invalid resolution {resolution} for row {json_data}')
      return
    row_resolver = row_resolvers[resolution]
    result = client.query(row_resolver, data=json_data)
    print_info(f'Row {json_data} resolved to {resolution} with result {result}')
  except Exception as e:
    print_err(f"Error executing query '{row_resolver}' with {json_data}")
    print_err(f"Exception: {e}")
  return


def run_bulk_queries(queries, no_transaction=False):
  if no_transaction:
    print_info('--no-transaction: running queries without transactional guarantees')
    for q in queries:
      try:
        client.query(q['__q'], **q['__v'])
      except Exception as e:
        print_err(f"Error executing query '{q['__q']}' with variables {json.dumps(q['__v'])}")
        print_err(f"Exception: {e}")
  else:
    for tx in client.transaction():
      with tx:
        for query_obj in queries:
          query = query_obj['__q']
          vars = query_obj['__v']
          try:
            tx.execute(query, **vars)
          except Exception as e:
            print_err(f"Error executing query '{query}' with variables {json.dumps(vars)}")
            raise e
