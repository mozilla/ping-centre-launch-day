#!/usr/bin/python
import psycopg2
import boto
from boto.s3.key import Key
from boto.utils import compute_md5
import tempfile
import os
import sys
import json
from psycopg2.extras import DictCursor


RS_HOST, RS_PORT, RS_DB, DASH_USER, DASH_PASSWORD, DASH_KEY_ID, DASH_ACCESS_KEY, DASH_BUCKET = \
    (x if x is not None else y for x, y in ((os.environ.get(x), y)
        for x, y in (('RS_HOST', '127.0.0.1'), ('RS_PORT', 5432), ('RS_DB', 'mozsplice'), ('DASH_USER', None),
                     ('DASH_PASSWORD', None), ('DASH_KEY_ID', None), ('DASH_ACCESS_KEY', None), ('DASH_BUCKET', None))))


BASE_QUERY = """
select count(distinct client_id) as total
from assa_sessions_daily
where date >= '2017-11-14' and version = '57.0' and release_channel = 'release'"""


def _connect(host='localhost', port=None, database=None, user='postgres', password=None):
    connection = psycopg2.connect(host=host, port=port, user=user, password=password, database=database)
    return connection, connection.cursor(cursor_factory=DictCursor)


def _query(cur, query_template, query_args=()):
    if query_args:
        query = query_template % query_args
    else:
        query = query_template
    cur.execute(query)
    for rec in cur:
        yield rec


def _upload_s3(datafile, key_id, access_key, bucket_name, key):
    with open(datafile) as f:
        md5 = compute_md5(f)

    conn = boto.connect_s3(key_id, access_key)
    bucket = conn.get_bucket(bucket_name, validate=False)

    k = Key(bucket)
    k.key = key

    k.set_contents_from_filename(datafile, md5=md5, replace=True)
    return "s3://%s/%s" % (bucket_name, k.key)


def run(event, context):
    _, curr = _connect(RS_HOST, RS_PORT, RS_DB, DASH_USER, DASH_PASSWORD)
    result = {'total': 0, 'countries': None}

    print("File: %s" % tmp.name)
    print("Query: %s" % BASE_QUERY)
    for record in _query(curr, BASE_QUERY):
        result['total'] = int(record)
        print record

    tmp = tempfile.NamedTemporaryFile(delete=False, prefix='dash', dir='/tmp')
    tmp.flush()
    tmp.close()

    s3file = _upload_s3(tmp.name, DASH_KEY_ID, DASH_ACCESS_KEY, DASH_BUCKET, "launch.json")

    print("Uploaded %s" % s3file)
    os.unlink(tmp.name)
    sys.stdout.flush()

    print("done")

