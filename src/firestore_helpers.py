from time import sleep
import firebase_admin
from google.cloud import firestore

from utils import print_success, print_warn

app = firebase_admin.initialize_app()
db = firestore.Client()

def encapsulate_metadata(obj):
  output_dict = obj.to_dict()
  output_dict['id'] = obj.id
  output_dict['create_time'] = obj.create_time
  output_dict['update_time'] = obj.update_time
  output_dict['_path'] = list(obj._reference._path)
  return output_dict


def to_list(stream):
  return list(map(encapsulate_metadata, stream))


def _fetch(collection: firestore.CollectionReference, limit=None, order_by=None, start_after=None):
  last_doc = None
  if order_by:
    collection = collection.order_by(order_by[0], direction=order_by[1])
  if start_after:
    if not order_by:
      raise Exception('When using start_after, please specify an order_by field.')
    collection = collection.start_after(start_after)
  if limit:
    collection = collection.limit(limit)
  result = collection.get()
  last_doc = result[-1] if result else None
  return {
    'result': to_list(result),
    'last_doc': last_doc,
  }


def fetch_collection(collection_name, limit=None, order_by=None, start_after=None):
  collection = db.collection(collection_name)
  last_doc = resolve_start_after(collection_name, start_after) if start_after else None
  return _fetch(collection, limit, order_by, last_doc)


def fetch_collection_group(collection_id, limit=None, order_by=None, start_after=None):
  collection = db.collection_group(collection_id)
  last_doc = resolve_start_after(collection_id, start_after, True) if start_after else None
  return _fetch(collection, limit, order_by, last_doc)



def fetch_single(collection_name, doc_id):
  doc_ref = db.collection(collection_name).document(doc_id)
  return doc_ref.get().to_dict()


def resolve_start_after(collection_name: str, doc, is_col_group=False):
  if type(doc) == str:
    print(f'start_after: interpreting "{doc}" as document key and retrieving it from {collection_name}...')
    if is_col_group:
      return db.collection_group(collection_name).document(doc)
    else:
      return db.collection(collection_name).document(doc).get()
  else:
    print(f"start_after: interpreting input value as a DocumentSnapshot.")
    return doc


def fetch_all(collection_name, order_by=None, start_after=None, page_size=100):
  if start_after and not order_by:
    raise Exception('When using start_after, please specify an order_by field.')
  
  if order_by and type(order_by) != tuple:
    raise Exception('order_by must be a tuple of (field, direction) where direction is either "ASCENDING" or "DESCENDING".')

  last_doc = resolve_start_after(collection_name, start_after) if start_after else None
  count = get_collection_count(collection_name, order_by, last_doc)
  page = 0
  docs = []

  print(f'fetch_all: Start fetching {count} documents from {collection_name}...')
  if order_by:
    print(f'fetch_all: Ordering by {order_by[0]}, {order_by[1]}.')
  while page * page_size < count:
    print(f'fetch_all: Fetch page {page} of size {page_size}...')
    if last_doc:
      print(f'fetch_all: Will start_after "{last_doc.id}".')
    response = fetch_collection(collection_name, page_size, order_by, last_doc)
    last_doc = response['last_doc']
    docs.extend(response['result'])
    print(f'fetch_all: Fetched {len(response["result"])} documents in page {page}, {len(docs)} so far.')
    page += 1

  if len(docs) == count:
    print_success(f'fetch_all: Finished fetching {len(docs)} documents, expected to find {count}.')
  else:
    print_warn(f'fetch_all: Expected to find {count} documents, but found {len(docs)}. You may have sorted on a field that is not defined for all docs.')
  return docs


def get_collection_count(collection_name, order_by=None, start_after=None):
  collection = db.collection(collection_name)
  if order_by:
    collection = collection.order_by(order_by[0], direction=order_by[1])
  if start_after:
    doc = resolve_start_after(collection_name, start_after)
    if not order_by:
      raise Exception('When using start_after, please specify an order_by field.')
    collection = collection.start_after(doc)
  return len(collection.get())
