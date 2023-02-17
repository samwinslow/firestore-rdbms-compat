import json
from math import isnan
import re
import numpy
import pandas
from google.api_core import datetime_helpers as g_datetime
from proto import datetime_helpers as p_datetime
from google.protobuf.timestamp_pb2 import Timestamp as p2_Timestamp
from colorama import Fore, Style


LIST_MIN_VALID = 0
LIST_MAX_VALID = 1024


def str_escape(value):
  return value.replace('\\', '\\\\').replace('"', '\\"')


def trim_whitespace(value):
  return re.sub(r'\s+', ' ', value)


def inspect(df):
  print(list(df))


def row_to_csv(row):
  row_string = map(lambda r: str(r).replace(',','\,'), row)
  return ','.join(row_string)


def to_list_of_dicts(df):
  l = df.to_dict('records')
  output = []
  for entry in l:
    for key, value in entry.items():
      if is_null(value):
        entry[key] = None
    output.append(entry)
  return output


def print_err(msg):
  print(Fore.RED + msg + Style.RESET_ALL)


def print_warn(msg):
  print(Fore.YELLOW + msg + Style.RESET_ALL)


def print_info(msg):
  print(Fore.CYAN + msg + Style.RESET_ALL)


def print_success(msg):
  print(Fore.GREEN + msg + Style.RESET_ALL)


# Transform function generator that returns first regex group match.
def regex_matcher(regex, default_none=False):
  if (not regex):
    raise Exception('Regex supplied to generator missing or invalid')
  def built_function(haystack=''):
    if (type(haystack) != str):
      return None
    match = re.search(regex, haystack)
    if match:
      return match.groups()[0]
    else:
      return None if default_none else haystack
  return built_function


def has_callable_transform(input_source):
  return 'transform' in input_source and callable(input_source['transform'])


def has_callable_resolver(input_source):
  if not ('resolve' in input_source):
    return False
  resolver_info = input_source['resolve']
  if not (type(resolver_info) == list and len(resolver_info) == 2):
    print('Resolver should be a list of length 2 with the first element being a callable and the second element being a dict of resolutions.')
    return False
  if not callable(resolver_info[0]):
    print('The resolver function at index 0 must be callable.')
    return False
  if not type(resolver_info[1]) == dict:
    print('Resolutions must be a dict in order to map return values of the resolver function to strings.') 
    return False
  return True


def build_resolver(field, resolver_info):
  resolve = resolver_info[0]
  resolutions = resolver_info[1]

  def built_function(value):
    if value is None or type(value) == float and isnan(value):
      return None
    resolution_key = resolve(value)
    if resolution_key in resolutions:
      return {
        '__sourceValue': value,
        '__query': resolutions[resolution_key],
        '__vars': { field: value },
      }
    else:
      raise Exception(f'Resolution key `{resolution_key}` not in dict, value: `{value}` (resolving {field})')

  return built_function


def jsonify(entries: list) -> str:
  source = entries
  output = []
  for entry in source:
    entry_resolved = entry
    if type(entry) == dict:
      for key, value in entry.items():
        if is_datetimewithnano(value):
          entry_resolved[key] = datetime_to_rfc3339(value)
    output.append(entry_resolved)
  return json.dumps(output)


def build_resolver_for_array(field_name, resolver_info):
  resolve = resolver_info[0]
  resolutions = resolver_info[1]

  def built_function(values = []):
    if type(values) != list:
      return None
    if len(values) <= LIST_MIN_VALID:
      return None
    if len(values) > LIST_MAX_VALID:
      print_warn(f'Will not build `{field_name}` subquery (len {len(values)} exceeds list size bounds)')
      return None
    source_values = []
    query = ':= assert_distinct({'
    for (i, entry) in enumerate(values):
      if type(entry) != dict:
        print_err('Not implemented')
        return None
      resolution_key = resolve(entry)
      if is_null(resolution_key):
        entry_id = entry.get('id', '<missing id>')
        print_err(f'Null resolution key for entry i={i} {entry_id} (resolving {field_name}).')
        source_values.append(None)
        continue
      if resolution_key in resolutions:
        source_values.append(entry)
        query += '(' + get_replace_query_num(resolutions[resolution_key], i) + '),'
      else:
        raise Exception(f'Resolution key `{resolution_key}` not in dict, value: `{entry}`')
    query += '})'
    if all(is_null(v) for v in source_values):
      print_info(f'All null values for `{field_name}` subquery, skipping')
      return None
    return {
      '__sourceValues': source_values,
      '__query': query,
      '__vars': { field_name: jsonify(source_values) },
    }

  return built_function


def get_replace_query_num(resolution: dict, n: int):
  q = resolution['query']
  if type(q) == str:
    return re.sub(r'%%n%%', str(n), q)


def safe_get_key(maybe_dict, key):
  if type(maybe_dict) == dict and key in maybe_dict:
    return maybe_dict[key]
  else:
    return None


def get_or_unnest_col(df, path):
  if type(path) == str:
    if path in df:
      return df[path]
    else:
      raise Exception(f'Column `{path}` not found in dataframe. Available columns: {list(df.columns)}')
  elif type(path) == list and len(path) == 2:
    return df[path[0]].apply(lambda v: safe_get_key(v, path[1]))
  else:
    raise Exception('Path for value access on df must be string or list of length 2.')


def append_col_to_df(source_df, col):
  if not col in source_df:
    print_info(f'Appending column "{col}" to source dataframe. If this is unexpected, check the source df for typos or missing columns.')
    source_df[col] = numpy.nan
  return source_df


def get_by_field(source_df, field, value):
  result = source_df[source_df[field] == value]
  return result


external_callables = ['get_time_zone', 'get_location']

def transform_source(
  source_df: pandas.DataFrame,
  source_mapping: dict,
  group_by: list,
  no_external: bool = False,
  single_column: str = None,
) -> pandas.DataFrame:
  output_df = pandas.DataFrame()

  def transform_col(col: str):
    append_col_to_df(output_df, col)
    input_source = source_mapping[col]
    if is_null(input_source):
      print_warn(f'Destination column "{col}" is null/empty in source mapping.')
      return
    if type(input_source) == str:
      output_df[col] = get_or_unnest_col(source_df, input_source)
    elif 'literal' in input_source:
      output_df[col] = input_source['literal']
    elif 'col' in input_source:
      input_col = get_or_unnest_col(source_df, input_source['col'])
      input_col_transformed = input_col
      if has_callable_transform(input_source):
        transform = input_source['transform']
        name = transform.__name__
        if not (name in external_callables and no_external):
          input_col_transformed = input_col.apply(transform)
      output_df[col] = input_col_transformed
    elif 'row' in input_source:
      transform = input_source.get('transform', None)
      if has_callable_transform(input_source):
        name = transform.__name__
        if name in external_callables and no_external:
          print_info(f'Skipping transform {name} because it calls an external service and --no-external was specified.')
          return
        output_df[col] = source_df.apply(transform, axis='columns')[col]
      else:
        raise Exception('A transform function must be specified when using `row`.')
    else:
      print_err('Unrecognized input source: {}'.format(input_source))
    if has_callable_resolver(input_source):
      resolver_info = input_source['resolve']
      resolver = None
      if 'type' in input_source and input_source['type'] == 'array':
        resolver = build_resolver_for_array(col, resolver_info)
      else:
        resolver = build_resolver(col, resolver_info)
      output_df[col] = output_df[col].apply(resolver)

  if is_null(single_column):
    for col in source_mapping:
      transform_col(col)
  else:
    transform_col(single_column)

  if not is_null(group_by):
    output_df = output_df.groupby(group_by)
  return output_df


zulu_regex = re.compile(r'(Z)|(\.\d+)$')
phone_regex = re.compile(r'^\+\d+$')
phone_noncompliant_chars = re.compile(r'[^\+\d]')


def is_user_irrelevant(user: dict):
  if user.get('isDeleted', False):
    return True
  if user.get('testingAccount', False):
    return True
  return False


def has_valid_name(user: dict):
  return bool(user.get('firstName', user.get('first_name', None)))


def is_phone_number(value: str):
  return re.match(phone_regex, value)


def fix_phone_number(value: str):
  if is_null(value) or is_phone_number(value):
    return value
  else:
    print_warn(f'fix_phone_number: will attempt to fix "{value}"')
    value = value.strip()
    if not value.startswith('+'):
      value = '+' + value
    value = re.sub(phone_noncompliant_chars, '', value)
    if is_phone_number(value):
      return value
    else:
      raise ValueError(f'fix_phone_number: "{value}" is not a valid phone number')


def is_null(expr):
  return type(expr) != list and (pandas.isnull(expr) or expr == '')


def fix_zulu_offset(value):
  if type(value) == str:
    return re.sub(zulu_regex, '+00:00', value)
  else:
    return value


def fix_int(floating_int):
  value = numpy.nan_to_num(floating_int)
  return int(value)


def has_dict_values(dictlike):
  if is_null(dictlike) or type(dictlike) != dict:
    return False
  elif any(dictlike.values()):
    return True
  else:
    return False


def is_datetimewithnano(value):
  return type(value) == g_datetime.DatetimeWithNanoseconds or type(value) == p_datetime.DatetimeWithNanoseconds


def datetime_to_rfc3339(value):
  if is_null(value):
    return None
  if type(value) == g_datetime.DatetimeWithNanoseconds:
    return fix_zulu_offset(g_datetime.to_rfc3339(value))
  elif type(value) == p_datetime.DatetimeWithNanoseconds:
    return fix_zulu_offset(value.rfc3339())
  elif type(value) == p2_Timestamp:
    return fix_zulu_offset(value.ToJsonString())
  elif type(value) == pandas.Timestamp:
    return fix_zulu_offset(value.isoformat(timespec='microseconds'))
  elif type(value) == str:
    return fix_zulu_offset(value)
  elif type(value) == dict:
    sec = value.get('seconds', value.get('_seconds', None))
    ns = value.get('nanoseconds', value.get('_nanoseconds', None))
    if is_null(sec) and is_null(ns):
      return None
    value = pandas.Timestamp(sec, unit='s', nanosecond=ns)
    return fix_zulu_offset(value.isoformat(timespec='microseconds'))
  else:
    raise Exception(f'Value "{value}" is not a supported type ({type(value)})')

