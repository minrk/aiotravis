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
import pickle
from pprint import pprint
import sqlite3
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
    def __init__(self, path=':memory:'):
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
                data json
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

    def save_build(self, build):
        repo = build['repository']['slug']
        now = datetime.utcnow()
        number = int(build['number'])

        self.db.execute(
            f"""
            INSERT INTO builds
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
        self.db.commit()

    def save_job(self, job):
        repo = job['repository']['slug']
        now = datetime.utcnow()
        build_number, job_number = [ int(s) for s in job['number'].split('.') ]
        self.db.execute(
            f"""
            INSERT INTO jobs
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
        self.db.commit()


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
        self.semaphore = asyncio.Semaphore(concurrency)
        self._session = None
        self.cache_age = cache_age
        # separate http and data caches,
        # as expiry is different
        # run all caches in a background thread since they talk to sqlite
        self.cache_thread = ThreadPoolExecutor(1)
        self._cache_file = cache_file
        self._init_caches()
        # self.cache_thread.submit(self._init_caches).result()

    def _init_caches(self):
        self.http_cache = Cache(self._cache_file, table='http')
        # self.http_cache.cull(cache_age)
        # long-term storage for things that shouldn't expire
        # e.g. finished builds
        self.data_store = DataStore(self._cache_file)

    def _in_thread(self, work):
        return asyncio.get_event_loop().run_in_executor(
            self.cache_thread,
            work,
    )

    async def cached_get(self, url, params=None):
        cache_key = url
        if params:
            # deterministic params in cache key
            # don't care if it's valid, so long as it's consistent
            cache_key += '?' + json.dumps(params, sort_keys=True)

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
        async with self.semaphore, aiohttp.ClientSession() as session:
            if self.debug:
                full_url = url + '?' + urlencode(params or {})
                print(f"Fetching {full_url}")
            async with session.get(
                url,
                params=params,
                headers=headers,
            ) as r:
                data = await r.json()
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
        # asyncio.ensure_future(self._in_thread(
        #     lambda: self.http_cache.set(cache_key, cached)
        # ))
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
        params.setdefault('limit', 100)
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

    async def all_builds(self, owner, build_state=None):
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
            cached_jobs = self.data_store.get_jobs(
                repo['slug'], n)
            cached_job_numbers = {job['number'] for job in cached_jobs}
            if self.debug:
                print(f"Cached {len(cached_jobs)}/{len(build['jobs'])} jobs for {repo['slug']}#{build['number']}")
            if len(cached_jobs) == len(build['jobs']):
                build['real_jobs'] = cached_jobs
            else:
                jobs = build['real_jobs'] = []
                async for job in self.jobs(build):
                    jobs.append(job)
                    # save finished or canceled jobs in long-term storage
                    if job['number'] not in cached_job_numbers:
                        if (job['state'] in {'canceled', 'errored'} or job['finished_at']):
                            self.data_store.save_job(job)
                        elif job['state'] not in {'created'}:
                            print("Not saving job")
                            pprint(job)

        async def build_coro(repo):
            # if self.debug:
            #     print(f"Getting cached builds for {repo['slug']}")
            builds = []
            cached_builds = self.data_store.get_builds(repo['slug'])
            cached_build_numbers = {build['number'] for build in cached_builds}
            seen = {0}
            offset = 0

            job_futures = []

            for build in cached_builds:
                # start offset with the last contiguous
                n = int(build['number'])
                job_futures.append(asyncio.ensure_future(job_coro(build)))
                if n - 1 in seen:
                    # contiguous numbering
                    seen.add(n)
                    offset = n
                    if build_state is None or build_state == build['state']:
                        builds.append(build)
                else:
                    break
            # collect job futures partway through
            await asyncio.gather(*job_futures)
            job_futures = []
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
                    job_futures.append(asyncio.ensure_future(job_coro(build)))
                    # store finished builds in long-term storage
                    # some builds are known to be in a bad state,
                    # so include stale/stuck builds in long-term storage
                    if build['number'] not in cached_build_numbers:
                        if (
                            build['finished_at'] or
                            parse_date(build['updated_at']) < \
                            datetime.now().astimezone(timezone.utc) - timedelta(days=180)
                        ):
                            self.data_store.save_build(build)
                        elif build['state'] not in {'created'}:
                            print("Not saving build")
                            pprint(build)

            await asyncio.gather(*job_futures)

            # if self.debug:
            #     print(f"Found {len(builds)} builds for {repo['slug']}[offset={offset}]")
            return builds

        async for repo in self.repos(owner):
            if repo['active']:
                build_futures.append(asyncio.ensure_future(build_coro(repo)))
        # after submitting all requests, start yielding builds
        for f in build_futures:
            for build in await f:
                yield build
