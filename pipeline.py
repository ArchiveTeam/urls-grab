# encoding=utf8
import datetime
from distutils.version import StrictVersion
import hashlib
import json
import os
import random
import shutil
import socket
import subprocess
import sys
import threading
import time
import string
import sys

if sys.version_info[0] < 3:
    from urllib import unquote
    from urlparser import parse_qs
else:
    from urllib.parse import unquote, parse_qs

import requests
import seesaw
from seesaw.config import realize, NumberConfigValue
from seesaw.externalprocess import WgetDownload
from seesaw.item import ItemInterpolation, ItemValue
from seesaw.pipeline import Pipeline
from seesaw.project import Project
from seesaw.task import SimpleTask, LimitConcurrent
from seesaw.tracker import GetItemFromTracker, PrepareStatsForTracker, \
    UploadWithTracker, SendDoneToTracker
from seesaw.util import find_executable
import zstandard

if StrictVersion(seesaw.__version__) < StrictVersion('0.8.5'):
    raise Exception('This pipeline needs seesaw version 0.8.5 or higher.')

LOCK = threading.Lock()


###########################################################################
# Find a useful Wget+Lua executable.
#
# WGET_AT will be set to the first path that
# 1. does not crash with --version, and
# 2. prints the required version string

WGET_AT = find_executable(
    'Wget+AT',
    [
	    'GNU Wget 1.20.3-at.20211001.01'
	],
    [
        './wget-at',
        '/home/warrior/data/wget-at'
    ]
)

if not WGET_AT:
    raise Exception('No usable Wget+At found.')


###########################################################################
# The version number of this pipeline definition.
#
# Update this each time you make a non-cosmetic change.
# It will be added to the WARC files and reported to the tracker.
VERSION = '20220123.01'
#USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.183 Safari/537.36'
TRACKER_ID = 'urls'
TRACKER_HOST = 'legacy-api.arpa.li'
MULTI_ITEM_SIZE = 40
MAX_DUPES_LIST_SIZE = 10000

###########################################################################
# This section defines project-specific tasks.
#
# Simple tasks (tasks that do not need any concurrency) are based on the
# SimpleTask class and have a process(item) method that is called for
# each item.
class CheckIP(SimpleTask):
    def __init__(self):
        SimpleTask.__init__(self, 'CheckIP')
        self._counter = 0

    def process(self, item):
        # NEW for 2014! Check if we are behind firewall/proxy

        if self._counter <= 0:
            item.log_output('Checking IP address.')
            ip_set = set()

            ip_set.add(socket.gethostbyname('twitter.com'))
            #ip_set.add(socket.gethostbyname('facebook.com'))
            ip_set.add(socket.gethostbyname('youtube.com'))
            ip_set.add(socket.gethostbyname('microsoft.com'))
            ip_set.add(socket.gethostbyname('icanhas.cheezburger.com'))
            ip_set.add(socket.gethostbyname('archiveteam.org'))

            if len(ip_set) != 5:
                item.log_output('Got IP addresses: {0}'.format(ip_set))
                item.log_output(
                    'Are you behind a firewall/proxy? That is a big no-no!')
                raise Exception(
                    'Are you behind a firewall/proxy? That is a big no-no!')

        # Check only occasionally
        if self._counter <= 0:
            self._counter = 10
        else:
            self._counter -= 1


class CheckRequirements(SimpleTask):
    def __init__(self):
        SimpleTask.__init__(self, 'CheckRequirements')
        self._checked = False

    def process(self, item):
        if not self._checked:
            assert shutil.which('pdftohtml') is not None
            self._checked = True


class PrepareDirectories(SimpleTask):
    def __init__(self, warc_prefix):
        SimpleTask.__init__(self, 'PrepareDirectories')
        self.warc_prefix = warc_prefix

    def process(self, item):
        item_name = item['item_name']
        item_name_hash = hashlib.sha1(item_name.encode('utf8')).hexdigest()
        escaped_item_name = item_name_hash
        dirname = '/'.join((item['data_dir'], escaped_item_name))

        if os.path.isdir(dirname):
            shutil.rmtree(dirname)

        os.makedirs(dirname)

        item['item_dir'] = dirname
        item['warc_file_base'] = '-'.join([
            self.warc_prefix,
            item_name_hash,
            time.strftime('%Y%m%d-%H%M%S')
        ])

        if not os.path.isfile('duplicate-urls.txt'):
            open('duplicate-urls.txt', 'w').close()

        open('%(item_dir)s/%(warc_file_base)s.warc.zst' % item, 'w').close()
        open('%(item_dir)s/%(warc_file_base)s_bad-urls.txt' % item, 'w').close()
        open('%(item_dir)s/%(warc_file_base)s_duplicate-urls.txt' % item, 'w').close()


class MoveFiles(SimpleTask):
    def __init__(self):
        SimpleTask.__init__(self, 'MoveFiles')

    def process(self, item):
        os.rename('%(item_dir)s/%(warc_file_base)s.warc.zst' % item,
            '%(data_dir)s/%(warc_file_base)s.%(dict_project)s.%(dict_id)s.warc.zst' % item)

        shutil.rmtree('%(item_dir)s' % item)


class SetBadUrls(SimpleTask):
    def __init__(self):
        SimpleTask.__init__(self, 'SetBadUrls')

    def unquote_url(self, url):
        temp = unquote(url)
        while url != temp:
            url = temp
            temp = unquote(url)
        return url

    def process(self, item):
        item['item_name_original'] = item['item_name']
        items = item['item_name'].split('\0')
        items_lower = [self.unquote_url(url).strip().lower() for url in item['item_urls']]
        with open('%(item_dir)s/%(warc_file_base)s_bad-urls.txt' % item, 'r') as f:
            for url in {
                self.unquote_url(url).strip().lower() for url in f
            }:
                index = items_lower.index(url)
                items.pop(index)
                items_lower.pop(index)
        item['item_name'] = '\0'.join(items)


class SetDuplicateUrls(SimpleTask):
    def __init__(self):
        SimpleTask.__init__(self, 'SetNewDuplicates')

    def process(self, item):
        with LOCK:
            self._process(item)

    def _process(self, item):
        with open('duplicate-urls.txt', 'r') as f:
            duplicates = {s.strip() for s in f}
        with open('%(item_dir)s/%(warc_file_base)s_duplicate-urls.txt' % item, 'r') as f:
            for url in f:
                duplicates.add(url.strip())
        with open('duplicate-urls.txt', 'w') as f:
            # choose randomly, to cycle periodically popular URLs
            duplicates = list(duplicates)
            random.shuffle(duplicates)
            f.write('\n'.join(duplicates[:MAX_DUPES_LIST_SIZE]))


class MaybeSendDoneToTracker(SendDoneToTracker):
    def enqueue(self, item):
        if len(item['item_name']) == 0:
            return self.complete_item(item)
        return super(MaybeSendDoneToTracker, self).enqueue(item)


def get_hash(filename):
    with open(filename, 'rb') as in_file:
        return hashlib.sha1(in_file.read()).hexdigest()

CWD = os.getcwd()
PIPELINE_SHA1 = get_hash(os.path.join(CWD, 'pipeline.py'))
LUA_SHA1 = get_hash(os.path.join(CWD, 'urls.lua'))

def stats_id_function(item):
    d = {
        'pipeline_hash': PIPELINE_SHA1,
        'lua_hash': LUA_SHA1,
        'python_version': sys.version,
    }

    return d


class ZstdDict(object):
    created = 0
    data = None

    @classmethod
    def get_dict(cls):
        if cls.data is not None and time.time() - cls.created < 1800:
            return cls.data
        response = requests.get(
            'https://legacy-api.arpa.li/dictionary',
            params={
                'project': TRACKER_ID
            }
        )
        response.raise_for_status()
        response = response.json()
        if cls.data is not None and response['id'] == cls.data['id']:
            cls.created = time.time()
            return cls.data
        print('Downloading latest dictionary.')
        response_dict = requests.get(response['url'])
        response_dict.raise_for_status()
        raw_data = response_dict.content
        if hashlib.sha256(raw_data).hexdigest() != response['sha256']:
            raise ValueError('Hash of downloaded dictionary does not match.')
        if raw_data[:4] == b'\x28\xB5\x2F\xFD':
            raw_data = zstandard.ZstdDecompressor().decompress(raw_data)
        cls.data = {
            'id': response['id'],
            'dict': raw_data
        }
        cls.created = time.time()
        return cls.data


class WgetArgs(object):
    def realize(self, item):
        with open('user-agents.txt', 'r') as f:
            USER_AGENT = random.choice(list(f)).strip()
        wget_args = [
            'timeout', '1000',
            WGET_AT,
            '-U', USER_AGENT,
            '-v',
            '--content-on-error',
            '--lua-script', 'urls.lua',
            '-o', ItemInterpolation('%(item_dir)s/wget.log'),
            '--no-check-certificate',
            '--output-document', ItemInterpolation('%(item_dir)s/wget.tmp'),
            '--truncate-output',
            '-e', 'robots=off',
            '--rotate-dns',
            '--recursive', '--level=inf',
            '--no-parent',
            '--timeout', '10',
            '--tries', '2',
            '--span-hosts',
            '--page-requisites',
            '--waitretry', '0',
            '--warc-file', ItemInterpolation('%(item_dir)s/%(warc_file_base)s'),
            '--warc-header', 'operator: Archive Team',
            '--warc-header', 'x-wget-at-project-version: ' + VERSION,
            '--warc-header', 'x-wget-at-project-name: ' + TRACKER_ID,
            '--warc-dedup-url-agnostic',
            '--warc-compression-use-zstd',
            '--warc-zstd-dict-no-include',
            '--header', 'Connection: keep-alive',
            '--header', 'Accept-Language: en-US;q=0.9, en;q=0.8'
        ]

        dict_data = ZstdDict.get_dict()
        with open(os.path.join(item['item_dir'], 'zstdict'), 'wb') as f:
            f.write(dict_data['dict'])
        item['dict_id'] = dict_data['id']
        item['dict_project'] = TRACKER_ID
        wget_args.extend([
            '--warc-zstd-dict', ItemInterpolation('%(item_dir)s/zstdict'),
        ])

        item['item_name_newline'] = item['item_name'].replace('\0', '\n')
        item_urls = []
        custom_items = {}

        item['item_name'] = '\0'.join(
            s for s in item['item_name'].split('\0')
            if not s.startswith('https://www.ukr.net/news/details/')
        )

        for item_name in item['item_name'].split('\0'):
            wget_args.extend(['--warc-header', 'x-wget-at-project-item-name: '+item_name])
            wget_args.append('item-name://'+item_name)
            if item_name.startswith('custom:'):
                data = parse_qs(item_name.split(':', 1)[1])
                for k, v in data.items():
                    if len(v) == 1:
                        data[k] = v[0]
                url = data['url']
                custom_items[url.lower()] = data
            else:
                url = item_name
            item_urls.append(url)
            wget_args.append(url)

        item['item_urls'] = item_urls
        item['custom_items'] = json.dumps(custom_items)

        if 'bind_address' in globals():
            wget_args.extend(['--bind-address', globals()['bind_address']])
            print('')
            print('*** Wget will bind address at {0} ***'.format(
                globals()['bind_address']))
            print('')

        return realize(wget_args, item)

###########################################################################
# Initialize the project.
#
# This will be shown in the warrior management panel. The logo should not
# be too big. The deadline is optional.
project = Project(
    title = 'URLs',
    project_html = '''
    <img class="project-logo" alt="logo" src="https://archiveteam.org/images/thumb/f/f3/Archive_team.png/235px-Archive_team.png" height="50px"/>
    <h2>Archiving sets of discovered outlinks. &middot; <a href="http://tracker.archiveteam.org/urls/">Leaderboard</a></span></h2>
    '''
)

pipeline = Pipeline(
    CheckIP(),
    CheckRequirements(),
    GetItemFromTracker('https://{}/{}/multi={}/'
        .format(TRACKER_HOST, TRACKER_ID, MULTI_ITEM_SIZE),
        downloader, VERSION),
    PrepareDirectories(warc_prefix='urls'),
    WgetDownload(
        WgetArgs(),
        max_tries=1,
        accept_on_exit_code=[0, 4, 8],
        env={
            'item_dir': ItemValue('item_dir'),
            'item_name': ItemValue('item_name_newline'),
            'custom_items': ItemValue('custom_items'),
            'warc_file_base': ItemValue('warc_file_base')
        }
    ),
    SetBadUrls(),
    SetDuplicateUrls(),
    PrepareStatsForTracker(
        defaults={'downloader': downloader, 'version': VERSION},
        file_groups={
            'data': [
                ItemInterpolation('%(item_dir)s/%(warc_file_base)s.warc.zst')
            ]
        },
        id_function=stats_id_function,
    ),
    MoveFiles(),
    LimitConcurrent(NumberConfigValue(min=1, max=20, default='2',
        name='shared:rsync_threads', title='Rsync threads',
        description='The maximum number of concurrent uploads.'),
        UploadWithTracker(
            'https://%s/%s' % (TRACKER_HOST, TRACKER_ID),
            downloader=downloader,
            version=VERSION,
            files=[
                ItemInterpolation('%(data_dir)s/%(warc_file_base)s.%(dict_project)s.%(dict_id)s.warc.zst')
            ],
            rsync_target_source_path=ItemInterpolation('%(data_dir)s/'),
            rsync_extra_args=[
                '--recursive',
                '--partial',
                '--partial-dir', '.rsync-tmp',
                '--min-size', '1',
                '--no-compress',
                '--compress-level', '0'
            ]
        ),
    ),
    MaybeSendDoneToTracker(
        tracker_url='https://%s/%s' % (TRACKER_HOST, TRACKER_ID),
        stats=ItemValue('stats')
    )
)

