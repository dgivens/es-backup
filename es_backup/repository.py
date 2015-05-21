import json
import re
import requests
import shutil
from es_backup.config import config
from es_backup.snapshot import Snapshot


class Repository(object):
    def __init__(self, name):
        base_url = config.get('default', 'base_url')
        self.name = name
        self.url = '%s/_snapshot/%s' % (base_url, self.name)

    def __exists(self):
        response = requests.get('%s' % self.url)
        if response.status_code < 300:
            return True
        return False

    def list_snapshots(self):
        response = requests.get('%s/_all' % self.url)
        data = response.json()
        snapshot_list = data['snapshots']
        snapshots = []
        for snapshot in snapshot_list:
            snapshots.append(Snapshot(snapshot['snapshot'], self))
        return snapshots

    def delete(self):
        response = requests.delete(self.url)
        if response.status_code >= 400:
            response.raise_for_status()


class FileRepository(Repository):
    def __init__(self, name, location=None, compress=True, chunk_size=None,
                 restore_rate='20mb', snapshot_rate='20mb'):
        base_url = config.get('default', 'base_url')
        self.name = name
        self.url = '%s/_snapshot/%s' % (base_url, name)
        if self.__get_repo() is False:
            self.type = 'fs'
            self.location = location
            self.compress = compress
            self.chunk_size = chunk_size
            self.restore_rate = restore_rate
            self.snapshot_rate = snapshot_rate
            self.__create_repo()

    def __str__(self):
        return 'repo %s - location: %s' % (self.name, self.location)

    def __get_repo(self):
        response = requests.get('%s' % self.url)
        if response.status_code < 300:
            data = response.json()
            self.type = data[self.name]['type']
            self.location = data[self.name]['settings'].get('location')
            self.compress = data[self.name]['settings'].get('compress')
            self.chunk_size = data[self.name]['settings'].get('chunk_size',
                                                              None)
            self.restore_rate = (data[self.name]['settings'].get(
                                 'max_restore_bytes_per_sec', '20mb'))
            self.snapshot_rate = (data[self.name]['settings'].get(
                                  'max_snapshot_bytes_per_sec', '20mb'))
            return True
        return False

    def __create_repo(self):
        repo_data = {
            'type': 'fs',
            'settings': {
                'location': self.location,
                'compress': self.compress,
                'chunk_size': self.chunk_size,
                'max_restore_bytes_per_sec': self.restore_rate,
                'max_snapshot_bytes_per_sec': self.snapshot_rate
            }
        }
        response = requests.put(self.url, data=json.dumps(repo_data))
        response.raise_for_status()

    def delete(self):
        response = requests.delete(self.url)
        if response.status_code >= 400:
            response.raise_for_status()
        shutil.rmtree(self.location)


class S3_Repository(Repository):
    def __init__(self, name):
        pass


class HDFS_Repository(Repository):
    def __init__(self, name):
        pass


class Azure_Repository(Repository):
    def __init__(self, name):
        pass


def list_repos(match=None):
    response = requests.get('%s/_snapshot/_all' % config.get('default',
                            'base_url'))
    data = response.json()
    if match:
        data = {repo: data[repo] for repo in data if re.match(match, repo)}
    repos = []
    for name in data:
        if data[name]['type'] == 'fs':
            repos.append(FileRepository(name))
        if data[name]['type'] == 's3':
            repos.append(S3_Repository(name))
        if data[name]['type'] == 'hdfs':
            repos.append(HDFS_Repository(name))
        if data[name]['type'] == 'azure':
            repos.append(Azure_Repository(name))
    return repos
