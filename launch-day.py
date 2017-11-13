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

# add total newtabs, total ios users, total pocket stories read

RS_HOST, RS_PORT, RS_DB, DASH_USER, DASH_PASSWORD, DASH_KEY_ID, DASH_ACCESS_KEY, DASH_BUCKET = \
    (x if x is not None else y for x, y in ((os.environ.get(x), y)
        for x, y in (('RS_HOST', '127.0.0.1'), ('RS_PORT', 5432), ('RS_DB', 'mozsplice'), ('DASH_USER', None),
                     ('DASH_PASSWORD', None), ('DASH_KEY_ID', None), ('DASH_ACCESS_KEY', None), ('DASH_BUCKET', None))))


TOTAL_QUERY = """
select count(distinct client_id) as total, count(*) as newtabs
from assa_sessions_daily
where version = '57.0' and release_channel = 'release'"""

POCKET_QUERY = """
select sum(clicks) as pocket_stories_read
from assa_impression_stats_daily
where version = '57.0' and release_channel = 'release'"""

COUNTRY_QUERY = """
select country_code, count(distinct client_id) as total
from assa_sessions_daily
where version = '57.0' and release_channel = 'release'
group by country_code"""


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

    k.set_metadata("Content-Type", "application/json")
    k.set_contents_from_filename(datafile, md5=md5, replace=True)
    return "s3://%s/%s" % (bucket_name, k.key)


def run(event, context):
    _, curr = _connect(RS_HOST, RS_PORT, RS_DB, DASH_USER, DASH_PASSWORD)
    result = {'total': 0, 'newtabs': 0, 'pocket_stories_read': 0, 'countries': {}}

    print("Query: %s" % TOTAL_QUERY)
    for record in _query(curr, TOTAL_QUERY):
        result['total'] = record['total']
        result['newtabs'] = record['newtabs']

    for record in _query(curr, POCKET_QUERY):
        result['pocket_stories_read'] = record['pocket_stories_read']

    for record in _query(curr, COUNTRY_QUERY):
        result['countries'][record['country_code']] = record['total']

    json_result = json.dumps(result)
    tmp = tempfile.NamedTemporaryFile(delete=False, prefix='dash', dir='/tmp')
    print("File: %s" % tmp.name)
    print("JSON: %s" % json_result)
    tmp.write(json_result)
    tmp.flush()
    tmp.close()

    s3file = _upload_s3(tmp.name, DASH_KEY_ID, DASH_ACCESS_KEY, DASH_BUCKET, "launch.json")

    print("Uploaded %s" % s3file)
    os.unlink(tmp.name)

    print("done")
    sys.stdout.flush()

if __name__ == '__main__':
    run(None, None)
