from resolvers import create, create_or_link, link, resolve_cohost, resolve_guest
from transforms import attach_metadata, get_and_fix_phone_number, get_created_at, get_flyer_fields, get_guest_uid, get_primary_cost, get_time_zone, get_updated_at, normalize_guest_status, normalize_local_date, simple_get_loc, unpack_tokens
from utils import datetime_to_rfc3339, fix_int, fix_phone_number, is_null


rules = {
  'flyers': {
    'fetch_order': ('eventId', 'ASCENDING'),
    'mapping': {
      'eventId': 'eventId',
      'backgroundImageURL': 'backgroundImageURL',
      'dimBackground': 'dimBackground',
      'height': {
        'col': ['dimensions', 'height'],
        'transform': fix_int,
      },
      'width': {
        'col': ['dimensions', 'width'],
        'transform': fix_int,
      },
      'font': 'font',
      'includedFields': {
        'row': True,
        'transform': get_flyer_fields,
      }
    },
    'edgedb_iterated_query': '''
      update Event
      filter .firebase_id = <str>item['eventId'] set {
        flyer := (insert Flyer {
          backgroundImageURL := <str>item['backgroundImageURL'],
          dimBackground := <bool>item['dimBackground'],
          height := <int16>item['height'],
          width := <int16>item['width'],
          font := <json>item['font'],
          includedFields := <array<str>>item['includedFields']
        })
      }
    '''
  },
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
  'posts': {
    'is_collection_group': True,
    'fetch_order': ('createdAt', 'ASCENDING'),
    'mapping': {
      'event_id': {
        'col': '_path',
        'transform': lambda x: x[1],
      },
      'firebase_id': 'id',
      'message': 'message',
      'created_at': {
        'col': 'create_time',
        'transform': datetime_to_rfc3339,
      },
      'updated_at': {
        'col': 'update_time',
        'transform': datetime_to_rfc3339,
      },
      'user_id': 'userId',
      'likes': {
        'col': 'likes',
        'transform': lambda d: list(d.keys()) if type(d) == dict else [],
      },
      'photos': {
        'col': 'photos',
        'transform': lambda l: l if type(l) == list else [],
      },
    },
    'group_by': ['event_id'],
    'edgedb_iterated_query': '''
      with json_data := <json>$data,
      unpacked_data := json_array_unpack(json_data),
      event_id := <str>array_agg(unpacked_data)[0]['event_id']
      update Event
      filter .firebase_id = event_id set {
        posts := distinct (
          for post in unpacked_data union (
            with users := (
              select User filter .firebase_uid = <str>post['user_id']
              limit 1
            )
            for user in users union (
              insert Post {
                author := user,
                firebase_id := <str>post['firebase_id'],
                message := <str>post['message'],
                created_at := to_datetime(<str>post['created_at']) ?? datetime_current(),
                updated_at := to_datetime(<str>post['updated_at']) ?? datetime_current(),
                likes := distinct (
                  with likes_data := <array<str>>post['likes'],
                  likes_unpacked := array_unpack(likes_data),
                  for l in likes_unpacked union (
                    with users := (
                      select detached User filter .firebase_uid = <str>l
                      limit 1
                    )
                    for user in users union (
                      insert PostLike {
                        user := user,
                      }
                    )
                  )
                ),
              } unless conflict on .firebase_id
            )
          )
        )
      }
    ''',
  },
  'guests': {
    'is_collection_group': True,
    'fetch_order': ('inviteTime', 'ASCENDING'),
    'mapping': {
      'event_id': {
        'col': '_path',
        'transform': lambda x: x[1],
      },
      'phone_number': {
        'row': True,
        'transform': get_and_fix_phone_number,
      },
      'status': {
        'col': 'status',
        'transform': normalize_guest_status,
      },
      'invite_time': {
        'col': 'inviteTime',
        'transform': datetime_to_rfc3339,
      },
      'firebase_uid': {
        'row': True,
        'transform': get_guest_uid,
      },
      'first_name': {
        'col': 'firstName',
        'transform': lambda x: x if not is_null(x) else None,
      },
      'last_name': {
        'col': 'lastName',
        'transform': lambda x: x if not is_null(x) else None,
      },
    },
    'row_resolver_function': resolve_guest,
    'edgedb_row_resolvers': {
      'link': '''
        with guest := <json>$data,
        g := (insert Guest {
          person := (
            select Person
            filter .phone_number = <str>guest['phone_number']
            limit 1
          ),
          status := <str>guest['status'],
          created_at := to_datetime(<str>guest['invite_time']) ?? datetime_current(),
        })
        update Event
        filter .firebase_id = <str>guest['event_id']
        set {
          guests += g
        };
      ''',
      'create_unregistered_user': '''
        with guest := <json>$data,
        g := (insert Guest {
          person := (
            insert UnregisteredUser {
              phone_number := <str>guest['phone_number'],
              first_name := <str>guest['first_name'],
              last_name := <str>guest['last_name'],
            }
          ),
          status := <str>guest['status'],
          created_at := to_datetime(<str>guest['invite_time']) ?? datetime_current(),
        })
        update Event
        filter .firebase_id = <str>guest['event_id']
        set {
          guests += g
        };
      ''',
      'create_user': '''
        with guest := <json>$data,
        g := (insert Guest {
          person := (
            insert User {
              phone_number := <str>guest['phone_number'],
              firebase_uid := <str>guest['firebase_uid'],
              first_name := <str>guest['first_name'],
              last_name := <str>guest['last_name'],
            }
          ),
          status := <str>guest['status'],
          created_at := to_datetime(<str>guest['invite_time']) ?? datetime_current(),
        })
        update Event
        filter .firebase_id = <str>guest['event_id']
        set {
          guests += g
        };
      ''',
    },
  },
  'events': {
    'fetch_order': ('host_id', 'ASCENDING'),
    'edgedb_table_name': 'Event',
    'edgedb_type_casts': {
      'start_time': 'datetime',
      'end_time': 'datetime',
      'created_at': 'datetime',
      'updated_at': 'datetime',
    },
    'edgedb_prereq_queries': [
      {
        'query': 'select count(User filter .firebase_uid = <str>$host)',
        'vars': {
          'host': 'host',
        }
      },
    ],
    'edgedb_query_suffix': 'unless conflict on .firebase_id',
    'mapping': {
      'location': {
        'col': 'location',
        'transform': simple_get_loc,
        'resolve': [
          create,
          {
            'create': '''
              := (
                with data := <json>$location
                insert EventLocation {
                  address := <str>data['address'],
                  apartment := <str>data['apartment'],
                  google_ref := <str>data['google_ref'],
                }
              )
            ''',
          }
        ]
      },
      'can_guests_invite': 'canInvite',
      'cohosts': {
        'col': 'cohosts',
        'type': 'array',
        'resolve': [
          resolve_cohost,
          {
            'link': {
              'query': '''
                  with data := <json>$cohosts,
                  cohost := data[%%n%%]
                  insert Cohost {
                    person := (
                      select Person
                      filter .phone_number = <str>cohost['phoneNumber']
                      limit 1
                    ),
                    status := <str>cohost['status'],
                  }
                ''',
            },
            'create_user': {
              'query': '''
                  with data := <json>$cohosts,
                  cohost := data[%%n%%]
                  insert Cohost {
                    person := (
                      insert User {
                        firebase_uid := <str>cohost['userId'],
                        phone_number := <str>cohost['phoneNumber'],
                        first_name := <str>cohost['firstName'],
                        last_name := <str>cohost['lastName'],
                      } unless conflict on .firebase_uid
                      else (
                        select Person
                        filter .phone_number = <str>cohost['phoneNumber']
                        limit 1
                      )
                    ),
                    status := <str>cohost['status'],
                  }
                ''',
            },
            'create_unregistered_user': {
              'query': '''
                  with data := <json>$cohosts,
                  cohost := data[%%n%%]
                  insert Cohost {
                    person := (
                      insert UnregisteredUser {
                        phone_number := <str>cohost['phoneNumber'],
                        first_name := <str>cohost['firstName'],
                        last_name := <str>cohost['lastName'],
                      } unless conflict on .phone_number
                      else (
                        select Person
                        filter .phone_number = <str>cohost['phoneNumber']
                        limit 1
                      )
                    ),
                    status := <str>cohost['status'],
                  }
                ''',
            },
          }
        ]
      },
      'description': 'description',
      'enable_payment': 'enablePayment',
      'end_time': 'end_time',
      'firebase_id': 'id',
      'host': {
        'col': 'host_id',
        'resolve': [
          link,
          {
            'link': ':= (select User filter .firebase_uid = <str>$host limit 1)',
          }
        ],
      },
      'image': {
        'col': ['image', 'url'],
        'resolve': [
          create,
          {
            'create': ':= (insert EventImage { url := <str>$image })',
          },
        ],
      },
      'is_cancelled': 'isCancelled',
      'name': 'name',
      'start_time': 'start_time',
      'time_zone_iana': {
        'row': True,
        'transform': get_time_zone,
      },
      'venmo_username': 'venmoUsername',
      'primary_cost': {
        'col': 'feeAmount',
        'transform': get_primary_cost,
      },
    },
  },
  'users': {
    'fetch_order': ('phoneNumber', 'ASCENDING'),
    # 'fetch_order': ('tokens', 'ASCENDING'),
    'edgedb_table_name': 'User',
    'edgedb_type_casts': {
      'birthday': 'cal::local_date',
      'created_at': 'datetime',
      'updated_at': 'datetime',
    },
    'edgedb_query_suffix': 'unless conflict on .firebase_uid',
    # 'skip_row_if_empty': ['tokens'],
    'mapping': {
      'bio': 'bio',
      'birthday': {
        'col': 'birthday',
        'transform': normalize_local_date,
      },
      'company': {
        'col': 'company_name',
        'resolve': [
          create_or_link('Company', 'name'),
          {
            'create': ':= (insert Company { name := <str>$company })',
            'link': ':= (select Company filter .name = <str>$company limit 1)',
          }
        ],
      },
      'created_at': {
        'row': True,
        'transform': get_created_at,
      },
      'firebase_uid': 'id',
      'first_name': {
        'col': 'first_name',
        'transform': lambda x: x if not is_null(x) else '',
      },
      'last_name': 'last_name',
      'location': 'location',
      'phone_number': {
        'col': 'phoneNumber',
        'transform': fix_phone_number,
      },
      'profile_image': {
        'col': ['profile_image', 'url'],
        'resolve': [
          create,
          {
            'create': ':= (insert ProfileImage { url := <str>$profile_image })',
          }
        ],
      },
      'school_name': 'school_name',
      'settings': 'settings',
      'snapchat': 'snapchat',
      'notification_tokens': {
        'col': 'tokens',
        'transform': unpack_tokens,
        'resolve': [
          create,
          {
            'create': '''
              := (
                with unpacked_data := json_array_unpack(<json>$notification_tokens)
                for entry in unpacked_data union (
                  insert NotificationToken {
                    device_id := <str>entry['device_id'],
                    token := <str>entry['token'],
                    created_at := to_datetime(<str>entry['created_at']) ?? datetime_current(),
                  }
                )
              )
            '''
          },
        ],
      },
      'updated_at': {
        'row': True,
        'transform': get_updated_at,
      },
      'metadata': {
        'row': True,
        'transform': attach_metadata,
      },
    }
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
