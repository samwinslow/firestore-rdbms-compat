# from resolvers import create, create_or_link, link, resolve_cohost, resolve_guest
# from transforms import attach_metadata, get_and_fix_phone_number, get_created_at, get_flyer_fields, get_guest_uid, get_primary_cost, get_time_zone, get_updated_at, normalize_guest_status, normalize_local_date, simple_get_loc, unpack_tokens
from utils import datetime_to_rfc3339, fix_int, fix_phone_number, is_null


rules = {
  'payments': {
    'is_collection_group': True,
    'fetch_order': ('paidTime', 'ASCENDING'),
    'mapping': {
      'event_id': {
        'col': '_path',
        'transform': lambda x: x[1],
      },
      'phone_number': {
        'col': 'id',
        'transform': fix_phone_number,
      },
      'paid': 'paid',
      'clicked_pay': 'clickedPay',
      'redirect_id': 'redirectId',
      'created_at': {
        'col': 'paidTime',
        'transform': datetime_to_rfc3339,
      },
    },
    'group_by': ['event_id'],
    'edgedb_iterated_query': '''
      with json_data := <json>$data,
      unpacked_data := json_array_unpack(json_data),
      event_id := <str>array_agg(unpacked_data)[0]['event_id']
      update Event
      filter .firebase_id = event_id set {
        payments := distinct (
          for payment in unpacked_data union (
            with persons := (
              select Person filter .phone_number = <str>payment['phone_number']
              limit 1
            )
            for person in persons union (
              insert Payment {
                paid := <bool>payment['paid'] ?? false,
                person := person,
                clicked_pay := <bool>payment['clicked_pay'],
                redirect_id := <str>payment['redirect_id'],
                created_at := to_datetime(<str>payment['created_at']) ?? datetime_current(),
              }
            )
          )
        )
      }
    ''',
  },
  'tokens': {
    'is_collection_group': True,
    'fetch_order': ('createdAt', 'ASCENDING'),
    'mapping': {
      'user_id': {
        'col': '_path',
        'transform': lambda x: x[1],
      },
      'device_id': {
        'col': '_path',
        'transform': lambda x: x[3],
      },
      'token': 'expoToken',
      'created_at': {
        'col': 'create_time',
        'transform': datetime_to_rfc3339,
      },
    },
    'group_by': ['user_id'],
    'edgedb_iterated_query': '''
      with data := <json>$data,
      unpacked_data := json_array_unpack(data),
      user_id := <str>array_agg(unpacked_data)[0]['user_id'],
      user := (
        select User
        filter .firebase_uid = user_id
          and count(.notification_tokens) = 0
        limit 1
      )
      update user set {
        notification_tokens := (
          for entry in unpacked_data union (
            insert NotificationToken {
              device_id := <str>entry['device_id'],
              token := <str>entry['token'],
              created_at := to_datetime(<str>entry['created_at']) ?? datetime_current(),
            }
          )
        )
      }
    '''
  }
}
