"""Travis API with asyncio

requires Python 3.6 for f-strings!

Caches requests with sqlite for performance.
"""

import os
from concurrent.futures import ThreadPoolExecutor
import asyncio
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import json
from pprint import pprint
import sqlite3
import time
from urllib.parse import quote, urlencode

import aiohttp
import dateutil.parser

TRAVIS = "https://api.travis-ci.org"

TRAVIS_CONFIG = os.path.expanduser('~/.travis/config.yml')

def parse_date(ds):
    if ds is None:
        return
    else:
        return dateutil.parser.parse(ds)

sqlite3.register_adapter(dict, json.dumps)
sqlite3.register_converter('json', json.loads)

def age_cutoff(age):
    """Create a cutoff date from an age

    age may be a timedelta or an integer number of seconds
    """
    if isinstance(age, int):
        age = timedelta(seconds=age)
    return datetime.utcnow() - age


class Cache():
    def __init__(self, path=":memory:", table='cache'):
        self.table = table
        self.db = sqlite3.connect(
            path,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
        )
        self.db.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table} (
                key text primary key,
                created timestamp,
                value json
            )
        """)
        self.db.commit()

    def get(self, key):
        """Get an item from the cache"""
        result = self.db.execute(
            f"""
            SELECT value, created FROM {self.table} WHERE
            key = ?
            LIMIT 1
            """,
            (key,)
        ).fetchone()
        if result:
            return result
        else:
            return None, None

    def set(self, key, value):
        """Store value in key"""
        now = datetime.utcnow()
        self.db.execute(
            f"""
            INSERT OR REPLACE INTO {self.table}
            (key, created, value)
            VALUES (?, ?, ?)
            """,
            (key, now, value),
        )
        self.db.commit()

    def cull(self, max_age):
        """Cull entries older than max_age

        max_age can be a timedelta or an integer number of seconds
        """
        cutoff = age_cutoff(max_age)
        self.db.execute(
            f"""
            DELETE FROM {self.table} WHERE
            created < ?)
            """,
            (cutoff,)
        )
        self.db.commit()


class DataStore():
    """Store long-term data (completed builds and jobs)

    Store completed builds and jobs here
    """
    def __init__(self, path=':memory:', commit_interval=100):
        self.commit_interval = commit_interval
        self._saves = 0
        self.db = sqlite3.connect(
            path,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
        )
        self.db.execute(
            f"""
            CREATE TABLE IF NOT EXISTS builds (
                repo text,
                number integer,
                retrieved timestamp,
                created_at timestamp,
                data json,
                PRIMARY KEY(repo, number)

            )
            """
        )
        self.db.execute(
            f"""
            CREATE TABLE IF NOT EXISTS jobs (
                repo text,
                build_number integer,
                job_number integer,
                retrieved timestamp,
                created_at timestamp,
                data json,
                PRIMARY KEY(repo, build_number, job_number),
                FOREIGN KEY(repo) REFERENCES builds(repo),
                FOREIGN KEY(build_number) REFERENCES builds(number)
            )
            """
        )
        self.db.execute(
            """
            CREATE INDEX IF NOT EXISTS build_repo ON builds(repo)
            """
        )
        self.db.execute(
            """
            CREATE INDEX IF NOT EXISTS job_repo ON jobs(repo)
            """
        )
        self.db.commit()

    def get_builds(self, repo):
        """Get all builds for a given repo"""
        return [row[0] for row in self.db.execute(
            f"""
            SELECT data FROM builds WHERE
            repo = ?
            ORDER BY number ASC
            """,
            (repo,)
        )]

    def get_jobs(self, repo, build_number):
        """Get all jobs for a given build"""
        return [row[0] for row in self.db.execute(
            f"""
            SELECT data FROM jobs WHERE
            repo = ? AND build_number = ?
            ORDER BY job_number ASC
            """,
            (repo, build_number)
        )]

    def save_build(self, build, commit=True):
        repo = build['repository']['slug']
        now = datetime.utcnow()
        number = int(build['number'])

        self.db.execute(
            f"""
            INSERT OR REPLACE INTO builds
            (repo, number, retrieved, created_at, data)
            VALUES
            (?   , ?     , ?        , ?         , ?   )
            """,
            (
                repo,
                number,
                now,
                build['commit']['committed_at'],
                build,
            ),
        )
        if commit:
            self.db.commit()

    def save_job(self, job, commit=True):
        repo = job['repository']['slug']
        now = datetime.utcnow()
        build_number, job_number = [ int(s) for s in job['number'].split('.') ]
        self.db.execute(
            f"""
            INSERT OR REPLACE INTO jobs
            (repo, build_number, job_number, retrieved, created_at, data)
            VALUES
            (?   , ?           , ?         , ?        , ?         , ?   )
            """,
            (
                repo,
                build_number,
                job_number,
                now,
                job['created_at'],
                job,
            )
        )
        if commit:
            self.db.commit()

    def all_builds(self, org):
        """Retrieve all builds for a given org

        As a single query
        Builds will be ordered by build number
        """
        builds = defaultdict(list)
        for repo, build in self.db.execute(
            """
            SELECT repo, data FROM builds
            WHERE repo LIKE ?
            ORDER BY number ASC
            """, (org + '/%',)
        ):
            builds[repo].append(build)
        return builds

    def all_jobs(self, org):
        """Retrieve all job for a given org

        As a single query, so that cache doesn't need to be hit many times.
        Jobs will be ordered by job number
        """
        jobs = defaultdict(lambda : defaultdict(list))
        for repo, build_number, job in self.db.execute(
            """
            SELECT repo, build_number, data FROM jobs
            WHERE repo LIKE ?
            ORDER BY build_number, job_number ASC
            """, (org + '/%',)
        ):
            jobs[repo][build_number].append(job)
        return jobs


class ProgressWidget():
    """Rate-limited progress widget

    avoids flood of messages since events happen quickly
    """
    def __init__(self, desc, interval=.01):
        from IPython.display import display
        from ipywidgets import HBox, IntProgress, HTML
        self._total = 0
        self.value = 0
        self._last_update = 0
        self.interval = interval
        prog = self.progress = IntProgress(description=desc, value=0, max=0)
        text = HTML()
        def _update_text(change):
            text.value = f"{prog.value}/{prog.max}"
        _update_text(None)
        prog.observe(_update_text, names=['value', 'max'])
        display(HBox([prog, text]))

    def update(self, n=1):
        self.value += n
        self._maybe_update()

    @property
    def total(self):
        return self._total

    @total.setter
    def total(self, value):
        self._total = value
        self._maybe_update()

    def _maybe_update(self):
        if self._total == self.progress.max and self.value == self.progress.value:
            return
        now = time.monotonic()
        if (now - self._last_update) >= self.interval:
            self._last_update = now
            self._update()

    def _update(self):
        self.progress.value = self.value
        self.progress.max = self._total


class Travis:
    def __init__(self, token=None, cache_file='travis.sqlite',
                 cache_age=3600, concurrency=20, debug=True):
        self.debug = debug
        self.skip_pages = False
        if token is None and os.getenv('TRAVIS_TOKEN'):
            token = os.getenv('TRAVIS_TOKEN')
        if token is None and os.path.exists(TRAVIS_CONFIG):
            # get token from travis
            from ruamel.yaml import YAML
            with open(TRAVIS_CONFIG) as f:
                cfg = YAML().load(f)
            for endpoint in cfg['endpoints']:
                if (endpoint.startswith(TRAVIS + '/')):
                    token = cfg['endpoints'][endpoint]['access_token']
        if token is None:
            raise ValueError("Specify travis token or setup travis gem and run `travis login`")
        self.headers = {
            'User-Agent': 'aiotravis',
            'Travis-API-Version': '3',
            'Accept': 'application/json',
            'Authorization': f'token {token}',
        }

        # semaphore to limit concurrent requests
        self.concurrency = concurrency
        self.semaphore = asyncio.Semaphore(concurrency)
        self._sessions = {}
        self.cache_age = cache_age
        # separate http and data caches,
        # as expiry is different
        # run all caches in a background thread since they talk to sqlite
        self.cache_thread = ThreadPoolExecutor(1)
        if cache_file != ':memory:':
            basename = os.path.basename(cache_file)
            base, ext = os.path.splitext(basename)
            http_cache = basename + '.http' + ext
        else:
            http_cache = cache_file
        self.http_cache = Cache(http_cache, table='http')
        # self.http_cache.cull(cache_age)
        # long-term storage for things that shouldn't expire
        # e.g. finished builds
        self.data_store = DataStore(cache_file)

    async def cached_get(self, url, params=None):
        cache_key = url
        full_url = url
        if params:
            # deterministic params in cache key
            # don't care if it's valid, so long as it's consistent
            # don't trust urlencode to preserve order
            cache_key += '?' + json.dumps(params, sort_keys=True)
        if params:
            full_url = url + '?' + urlencode(params)

        cached, cache_date = self.http_cache.get(cache_key)
        if cached and cache_date >= age_cutoff(self.cache_age):
            return cached['body']
        headers = {}
        headers.update(self.headers)
        if cached:
            # if we have an 'expired' cache entry,
            # still use etag to avoid consuming too much resources
            # this helps on GitHub, but may not with Travis.
            etag = cached['headers'].get('etag')
            if etag:
                headers['If-Not-Modified'] = etag
        async with self.semaphore:
            if not hasattr(self, 'session'):
                self.session = aiohttp.ClientSession()
            session = self.session
            if True or self.debug:
                print(f"Fetching {full_url}")
            async with session.get(
                url,
                params=params,
                headers=headers,
            ) as r:
                try:
                    data = await r.json()
                except Exception:
                    text = await r.text()
                    print(f"Failed to get JSON: {text}")
                    raise
                if r.status == 200:
                    # make headers pickleable and case-insensitive (lowercase)
                    headers = {
                        key.lower(): value
                        for key, value in r.headers.items()
                    }
                    cached = {
                        'headers': headers,
                        'body': data,
                    }
                elif r.status == 304:
                    pass
                    # re-use previous response,
                    # but update its date in the cache because it's still valid
                else:
                    raise ValueError(f"Unexpected response status: {r.status}")
        self.http_cache.set(cache_key, cached)
        return cached['body']

    def _pagination_progress(self, pagination, key, who_for):
        """Print progress of a paginated request"""
        so_far = pagination['offset'] + pagination['limit']
        total = pagination['count']
        if pagination['is_last']:
            so_far = total
        print(f"{so_far}/{total} {key} for {who_for}")

    async def paginated_get(self, url, params, who_for=''):
        """generator yielding paginated list from travis API"""
        if not params:
            params = {}
        # if self.debug:
        #     print('getting', url, params)
        data = await self.cached_get(url, params)
        key = data['@type']
        for item in data[key]:
            yield item
        pagination = data['@pagination']

        if self.debug:
            self._pagination_progress(pagination, key, who_for)

        if self.skip_pages or pagination['is_last']:
            # no more pages
            return


        # schedule paged requests in parallel
        requests = []
        limit = pagination['limit']
        so_far = pagination['offset'] + limit
        total = pagination['count']
        for offset in range(so_far, total + 1, limit):
            new_params = {}
            new_params.update(params)
            new_params['offset'] = offset
            requests.append(asyncio.ensure_future(
                self.cached_get(url, new_params)
            ))
        for f in requests:
            data = await f
            for item in data[key]:
                yield item
            pagination = data['@pagination']
            if self.debug:
                self._pagination_progress(pagination, key, who_for)

    async def repos(self, owner, **params):
        """Async generator yielding all repos owned by @owner"""
        futures = []
        url = f"{TRAVIS}/owner/{owner}/repos"
        async for repo in self.paginated_get(url, params, owner):
            yield repo

    async def builds(self, repo, **params):
        """Async generator yielding all builds for a repo"""
        if isinstance(repo, str):
            repo_name = quote(repo, safe='')
            repo = await self.cached_get(f"{TRAVIS}/repo/{repo_name}")
        url = f"{TRAVIS}/repo/{repo['id']}/builds"
        if self.debug:
            print(f"Fetching builds for {repo['slug']}")
        async for build in self.paginated_get(url, params, repo['slug']):
            yield build

    async def jobs(self, build):
        """Get all jobs for a build"""
        futures = []
        if self.debug:
            print(f"Fetching jobs for {build['repository']['slug']}#{build['number']}")
        who_for = f"{build['repository']['slug']}#{build['number']}"
        url = f"{TRAVIS}/build/{build['id']}/jobs"
        reply = await self.cached_get(url)
        for job in reply['jobs']:
            yield job

    def progress_widgets(self):
        """show progress with IPython widgets"""
        return {
            'repo': ProgressWidget('repos'),
            'build': ProgressWidget('builds'),

            'job': ProgressWidget('jobs'),
        }

    def progress_tqdm(self):
        import tqdm
        def progress(name):
            return tqdm.tqdm_notebook(desc=name, unit='')
        return {
            'repo': progress('repos'),
            'build': progress('builds'),
            'job': progress('jobs'),
        }

    async def all_builds(self, owner, build_state=None, progress=True):
        cache = {
            'builds': self.data_store.all_builds(owner),
            'jobs':  self.data_store.all_jobs(owner),
        }
        if progress:
            progress = self.progress_widgets()

        def _inc_total(key, by=1):
            if not progress:
                return
            progress[key].total += by

        def _inc_value(key, by=1):
            if not progress:
                return
            progress[key].update(by)

        build_futures = []
        # iterate through repos and submit requests for builds
        build_params = {'sort_by': 'id'}
        if build_state:
            build_params['state'] = build_state

        # spawn generators in coroutines so they run concurrently
        # otherwise, lazy evaluation of generators will serialize everything
        async def job_coro(build):
            repo = build['repository']
            n = int(build['number'])
            cached_jobs = cache['jobs'][repo['slug']][n]
            # self.data_store.get_jobs(
            #     repo['slug'], n)
            cached_job_numbers = {job['number'] for job in cached_jobs}
            _inc_total('job', len(build['jobs']))
            if len(cached_jobs) == len(build['jobs']):
                build['real_jobs'] = cached_jobs
                _inc_value('job', len(cached_jobs))
            elif len(cached_jobs) > len(build['jobs']):
                raise ValueError(f"Too many jobs for {repo['slug']}#{build['number']}!")
            else:
                jobs = build['real_jobs'] = []
                async for job in self.jobs(build):
                    jobs.append(job)
                    # save finished or canceled jobs in long-term storage
                    if job['number'] not in cached_job_numbers:
                        if (job['state'] in {'canceled', 'errored'} or job['finished_at']):
                            self.data_store.save_job(job, commit=True)
                        elif job['state'] not in {'created', 'started', 'pending'}:
                            print(f"Not saving {job['state']} job")
                            pprint(job)
                    _inc_value('job')

        async def build_coro(repo):
            # if self.debug:
            #     print(f"Getting cached builds for {repo['slug']}")
            builds = []
            cached_builds = cache['builds'][repo['slug']]
            # self.data_store.get_builds(repo['slug'])
            # cached_builds = []
            cached_build_numbers = {build['number'] for build in cached_builds}
            seen = {0}
            offset = 0
            job_futures = []

            async def add_job_future(build):
                if len(job_futures) >= self.concurrency:
                    await asyncio.gather(*job_futures)
                    job_futures[:] = []
                f = asyncio.ensure_future(job_coro(build))
                f.add_done_callback(lambda f: _inc_value('build'))
                job_futures.append(f)

            _inc_total('build', len(cached_builds))

            for build in cached_builds:
                # start offset with the last contiguous
                n = int(build['number'])
                await add_job_future(build)
                if n - 1 in seen:
                    # contiguous numbering
                    seen.add(n)
                    offset = n
                    if build_state is None or build_state == build['state']:
                        builds.append(build)
                else:
                    break
            params = {}
            params.update(build_params)
            # round offset to avoid cache busting
            if offset != 0:
                params['offset'] = offset - (offset % 100)
            # if self.debug:
            #     print(f"Fetching builds for {repo['slug']}[offset={offset}]")


            async for build in self.builds(repo, **params):
                if int(build['number']) > offset:
                    builds.append(build)
                    await add_job_future(build)
                    # store finished builds in long-term storage
                    # some builds are known to be in a bad state,
                    # so include stale/stuck builds in long-term storage
                    if build['number'] not in cached_build_numbers:
                        _inc_total('build')
                        if (
                            build['finished_at'] or
                            parse_date(build['updated_at']) < \
                            datetime.now().astimezone(timezone.utc) - timedelta(days=180)
                        ):
                            self.data_store.save_build(build, commit=True)
                        elif build['state'] not in {'created', 'started'}:
                            print("Not saving build")
                            pprint(build)
            if job_futures:
                await asyncio.gather(*job_futures)
            # if self.debug:
            #     print(f"Found {len(builds)} builds for {repo['slug']}[offset={offset}]")
            self.data_store.db.commit()
            return builds

        async for repo in self.repos(owner):
            _inc_total('repo')
            if repo['active']:
                build_futures.append(asyncio.ensure_future(build_coro(repo)))
        # after submitting all requests, start yielding builds
        while build_futures:
            done, build_futures = await asyncio.wait(build_futures, return_when=asyncio.FIRST_COMPLETED)
            _inc_value('repo', len(done))
            for f in done:
                for build in await f:
                    yield build


def load_builds(org, db_file='travis.sqlite'):
    import pandas as pd
    conn = sqlite3.connect('travis.sqlite')
    all_builds = conn.execute(
                """
        SELECT data FROM builds
        WHERE repo LIKE ?
        ORDER BY created_at, repo
        """,
        (org + '/%',)
    ).fetchall()
    columns = [
        'org',
        'name',
        'number',
        'state',
        'started',
        'finished',
        'created',
        'duration',
    ]
    rows = []
    for (build,) in all_builds:
        build = json.loads(build)
        org, name = build['repository']['slug'].split('/')
        row = [
            org,
            name,
            int(build['number']),
            build['state'],
            parse_date(build['started_at']),
            parse_date(build['finished_at']),
            parse_date(build['commit']['committed_at']),
            build['duration'],
        ]
        rows.append(row)
    return pd.DataFrame(rows, columns=columns)



def load_jobs(org, db_file='travis.sqlite'):
    import pandas as pd
    conn = sqlite3.connect('travis.sqlite')
    all_jobs = conn.execute(
        """
        SELECT data FROM jobs
        WHERE repo LIKE ?
        ORDER BY created_at, repo
        """,
        (org + '/%',)
    ).fetchall()
    columns = [
        'org',
        'name',
        'build',
        'number',
        'state',
        'started',
        'finished',
        'created',
    ]
    rows = []
    for (job,) in all_jobs:
        job = json.loads(job)
        org, name = job['repository']['slug'].split('/')
        build, number = (int(part) for part in job['number'].split('.'))
        row = [
            org,
            name,
            build,
            number,
            job['state'],
            parse_date(job['started_at']),
            parse_date(job['finished_at']),
            parse_date(job['created_at']),
        ]
        rows.append(row)
    return pd.DataFrame(rows, columns=columns)

