import json
import requests
from dateutil.parser import *


class Snapshot(object):
    def __init__(self, name, repo, indices='_all', ignore_unavailable=False,
                 include_global_state=True, partial=False):
        self.name = name
        self.repo = repo
        self.url = '%s/%s' % (repo.url, self.name)
        self.indices = indices
        self.ignore_unavailable = ignore_unavailable
        self.include_global_state = include_global_state
        self.partial = partial

        if self.__get_snapshot() is False:
            self.__create_snapshot()
            self.__get_snapshot()

    def __str__(self):
        return 'Snapshot %s of repo %s' % (self.name, self.repo.name)

    def __get_snapshot(self):
        response = requests.get(self.url)
        if response.status_code < 300:
            data = response.json()
            snapshot = data['snapshots'][0]
            indices = ','.join(snapshot.get('indices'))
            if indices is '':
                self.indices = '_all'
            else:
                self.indices = indices
            self.state = snapshot.get('state')
            self.start_time = parse(snapshot.get('start_time'))
            self.end_time = parse(snapshot.get('end_time'))
            self.duration = snapshot.get('duration_in_millis') / 1000.0
            self.failures = snapshot.get('failures')
            self.shards = snapshot.get('shards')
            return True
        return False

    def __create_snapshot(self):
        snapshot_data = {
            'indices': self.indices,
            'ignore_unavailable': self.ignore_unavailable,
            'include_global_state': self.include_global_state,
            'partial': self.partial
        }
        response = requests.put(self.url, data=json.dumps(snapshot_data))

    def update_status(self):
        self.__get_snapshot()

    def delete(self):
        response = requests.delete(self.url)
        if response.status_code >= 400:
            response.raise_for_status()

    def restore(self):
        pass
