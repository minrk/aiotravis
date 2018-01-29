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
import sqlite3
from urllib.parse import quote

import aiohttp

TRAVIS = "https://api.travis-ci.org"

TRAVIS_CONFIG = os.path.expanduser('~/.travis/config.yml')


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
                started_at timestamp,
                finished_at timestamp,
                duration integer,
                data json
            )
            """
        )
        self.db.execute(
            """
            CREATE INDEX IF NOT EXISTS build_repo ON builds(repo)
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

    def save_build(self, build):
        repo = build['repository']['slug']
        now = datetime.utcnow()
        data = json.dumps(build)
        number = int(build['number'])

        self.db.execute(
            f"""
            INSERT INTO builds
            (repo, number, retrieved, created_at, started_at,
            finished_at, duration, data)
            VALUES
            (?   , ?     , ?        , ?         , ?         ,
            ?          , ?       , ?   )
            """,
            (
                repo,
                number,
                now,
                build['commit']['committed_at'],
                build['started_at'],
                build['finished_at'],
                build['duration'],
                build,
            ),
        )
        self.db.commit()

class Travis:
    def __init__(self, token=None, cache_file='travis.sqlite',
                 cache_age=3600, concurrency=20, debug=True):
        self.debug = debug
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

        if pagination['is_last']:
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
        async for build in self.paginated_get(url, params, repo['slug']):
            yield build

    async def jobs(self, build):
        """Get all jobs for a build"""
        for job_info in build['jobs']:
            job = await self.job(job_info)
            yield job

    async def job(self, job_info):
        url = f"{TRAVIS}{job_info['@href']}"
        job = await self.cached_get(url)
        print(job['state'])
        return job

    async def all_builds(self, owner, build_state=None):
        build_futures = []
        # iterate through repos and submit requests for builds
        build_params = {'sort_by': 'id'}
        if build_state:
            build_params['state'] = build_state
        # spawn generators in coroutines so they run concurrently
        # otherwise, lazy evaluation of generators will serialize everything

        async def build_coro(repo):
            # if self.debug:
            #     print(f"Getting cached builds for {repo['slug']}")
            builds = []
            cached_builds = self.data_store.get_builds(repo['slug'])
            # await self._in_thread(
            #     lambda:
            # )
            seen = {0}
            offset = 0
            for build in cached_builds:
                # start offset with the last contiguous
                n = int(build['number'])
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
                    # store finished builds in long-term storage
                    if build['finished_at']:
                        self.data_store.save_build(build)
                    # if build['finished_at']:
                    #     asyncio.ensure_future(self._in_thread(
                    #         lambda:
                    #     ))
            # if self.debug:
            #     print(f"Found {len(builds)} builds for {repo['slug']}[offset={offset}]")
            # self.data_store.db.commit()
            return builds

        async for repo in self.repos(owner):
            if repo['active']:
                build_futures.append(asyncio.ensure_future(build_coro(repo)))
        # after submitting all requests, start yielding builds
        for f in build_futures:
            for build in await f:
                yield build
