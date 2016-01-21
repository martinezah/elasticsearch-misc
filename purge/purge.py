#!/usr/bin/env python

import argparse
import datetime
import requests
import sys
import time

ES_URL = 'http://localhost:9200'
ES_REPO = ''
ES_USER = ''
ES_PASS = ''
RETENTION_DAYS = 14

# load settings
try:
    from local_settings import *
except:
    pass

# process commandline args
parser = argparse.ArgumentParser(description='Purge elasticsearch snapshots')
parser.add_argument('--url', help='Elasticsearch API URL')
parser.add_argument('--username', help='Elasticsearch HTTP basic auth username')
parser.add_argument('--password', help='Elasticsearch HTTP basic auth username')
parser.add_argument('--repository', help='Elasticsearch snapshot repository name')
parser.add_argument('--days', help='Number of days to retain snapshots')
args = parser.parse_args()

if args.url:
    ES_URL = args.url
if args.username:
    ES_USER = args.username
if args.password:
    ES_PASS = args.password
if args.repository:
    ES_REPO = args.repository
if args.days:
    RETENTION_DAYS = int(args.days)

counts = {'total': 0, 'purged': 0, 'retained': 0, 'failed': 0}

# retrieve snapshot info
print 'Getting snapshot data...'
rr = requests.get('{}/_snapshot/{}/_all'.format(ES_URL, ES_REPO), auth=(ES_USER, ES_PASS))
if rr.status_code != 200:
    print 'ERROR: could not retrieve snapshot data; aborting'
    sys.exit(1)    
_snapshots = rr.json()

# note date of most recent success
last_success = None
for snapshot in _snapshots['snapshots']:
    counts['total'] += 1
    if snapshot['state'] == 'SUCCESS':
        snapshot_start = int(snapshot['start_time_in_millis'])
        if last_success is None or snapshot_start > last_success:
                last_success = snapshot_start

# if we don't have at least one clean snapshot, abort
if last_success is None:
    print 'ERROR: no successful snapshots; aborting'
    sys.exit(1)
    
last_success_date = datetime.datetime.fromtimestamp(int(last_success / 1000)).strftime('%c')
print 'Found {} snapshots; last successful snapshot was {}'.format(counts['total'], last_success_date)
    
# if most recent success is within age range, delete all snapshots outside age range
# otherwise, delete all snapshots prior to most recent success
purge_date = int(time.time() * 1000) - (RETENTION_DAYS * 24 * 60 * 60 * 1000)

if last_success < purge_date:
    purge_date = last_success
    print "WARNING: no successful snapshots in retention range"
for snapshot in _snapshots['snapshots']:
    if int(snapshot['start_time_in_millis']) < purge_date:
        print 'Purging {}'.format(snapshot['snapshot'])
        rr = requests.delete('{}/_snapshot/{}/{}'.format(ES_URL, ES_REPO, snapshot['snapshot']), auth=(ES_USER, ES_PASS))
        if (rr.status_code == 200):
            counts['purged'] += 1
        else:
            print rr.text
            counts['failed'] += 1
    else:        
        counts['retained'] += 1
        
print 'Done; {retained} retained, {purged} purged, {failed} failed to purge'.format(**counts)
