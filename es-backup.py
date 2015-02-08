#!/usr/bin/env python

import argparse
import sys
from es_backup.repository import *
from es_backup.snapshot import *
from es_backup.backup import *
from jinja2 import Environment, PackageLoader


def render_template(template, **variables):
    env = Environment(loader=PackageLoader('es_backup', 'templates'))
    template = env.get_template(template)
    print(template.render(**variables))


class Commands(object):
    def __init__(self):
        parser = argparse.ArgumentParser(description='Elasticsearch backup '
                                         'management utility',
                                         usage=('''es-backup.py <command> [<args>]

Commands
    repo-list
    repo-details
    repo-create
    repo-delete
    snapshot-list
    snapshot-details
    snapshot-create
    snapshot-delete
    scheduled-backup
'''))
        parser.add_argument('command', help='Subcommand to run')
        args = parser.parse_args(sys.argv[1:2])
        command = args.command.replace('-', '_')
        if not hasattr(self, command):
            print('Unrecognized command')
            parser.print_help()
            sys.exit(1)
        func = getattr(self, command)
        func()

    def __arg_conf(self, argv=None, config=None):
        if argv is None:
            return config
        return argv

    def repo_list(self):
        repos = list_repos()
        render_template('repo_list', repos=repos)

    def repo_details(self):
        parser = argparse.ArgumentParser(description='Show repository details')
        parser.add_argument('name', help='Name of repository')
        parser.add_argument('-t', '--type', default='fs', choices=['fs', 's3',
                            'azure', 'hdfs'], help='Repository type (fs, s3, '
                            'azure, hdfs)')
        args = parser.parse_args(sys.argv[2:])
        name = args.name
        if args.type == 'fs':
            repo = FileRepository(name)
        if args.type == 's3':
            repo = S3_Repository(name)
        if args.type == 'azure':
            repo = Azure_Repository(name)
        if args.type == 'hdfs':
            repo = HDFS_Repository(name)
        render_template('repo_details', repo=repo)

    def __fs_repo_create(self, args):
        compress = self.__arg_conf(args.compress,
                                   config.get('fs', 'compress'))
        chunk_size = self.__arg_conf(args.chunk_size,
                                     config.get('fs', 'chunk_size'))
        if chunk_size == 'null':
            chunk_size = None

        restore_rate = self.__arg_conf(args.restore_rate,
                                       config.get('fs', 'restore_rate'))
        snapshot_rate = self.__arg_conf(args.snapshot_rate,
                                        config.get('fs', 'snapshot_rate'))

        repo = FileRepository(args.name, location=args.location,
                              compress=compress, chunk_size=chunk_size,
                              restore_rate=restore_rate,
                              snapshot_rate=snapshot_rate)
        if repo:
            print('Filesystem repository %s created at %s' % (repo.name,
                                                              repo.location))

    def __s3_repo_create(self, args):
        pass

    def __azure_repo_create(self, args):
        pass

    def __hdfs_repo_create(self, args):
        pass

    def repo_create(self):
        parser = argparse.ArgumentParser(description='Create snapshot '
                                         'repository')
        subparsers = parser.add_subparsers(help='repository type')

        # Filesystem
        parser_fs = subparsers.add_parser('fs')
        parser_fs.add_argument('name', help='Name of repository')
        parser_fs.add_argument('-l', '--location', required=True, help='Path '
                               'of repository')
        parser_fs.add_argument('-c', '--compress', action='store_true',
                               help='Enable metadata file compression '
                               '(Default: true)')
        parser_fs.add_argument('-C', '--chunk-size', help='Break large files '
                               'into smaller chunks (Example: 1g, 10m, 5k')
        parser_fs.add_argument('-r', '--restore-rate', help='Max rate of '
                               'restore (Default: 20mb)')
        parser_fs.add_argument('-s', '--snapshot-rate', help='Max rate of '
                               'snapshot creation (Default: 20mb)')
        parser_fs.set_defaults(func=self.__fs_repo_create)

        # S3
        parser_s3 = subparsers.add_parser('s3')
        parser_s3.add_argument('name', help='Name of repository')
        parser_s3.add_argument('-b', '--bucket', required=True, help='S3 '
                               'bucket for repository')
        parser_s3.add_argument('-r', '--region', help='Region (Default: '
                               'us-east-1)')
        parser_s3.add_argument('-e', '--endpoint', help='S3 API endpoint '
                               '(Default: s3.amazonaws.com)')
        parser_s3.add_argument('-p', '--protocol', help='HTTP protocol '
                               '(Default: https)')
        parser_s3.add_argument('-B', '--base-path', help='Path for the '
                               'repository within the bucket')
        parser_s3.add_argument('-a', '--access-key', help='Access key for '
                               'auth')
        parser_s3.add_argument('-s', '--secret-key', help='Secret key for '
                               'auth')
        parser_s3.add_argument('-c', '--compress', action='store_true',
                               help='Enable metadata file compression')
        parser_s3.add_argument('-C', '--chunk-size', help='Splits large files '
                               'into chunks (Default: 100m)')
        parser_s3.add_argument('-E', '--server-side-encryption',
                               action='store_true', help='Enable AES256 '
                               'encryption in repo')
        parser_s3.add_argument('--buffer-size', help='Minimum threshold below '
                               'which the chunk is uploaded using a single '
                               'request (Default 5mb)')
        parser_s3.add_argument('--max-retries', help='Number of retries in '
                               'case of S3 error')
        parser_s3.set_defaults(func=self.__s3_repo_create)

        # Azure
        parser_azure = subparsers.add_parser('azure')
        parser_azure.add_argument('name', help='Name of repository')
        parser_azure.set_defaults(func=self.__azure_repo_create)

        # HDFS
        parser_hdfs = subparsers.add_parser('hdfs')
        parser_hdfs.add_argument('name', help='Name of repository')
        parser_hdfs.set_defaults(func=self.__hdfs_repo_create)

        args = parser.parse_args(sys.argv[2:])
        args.func(args)

    def repo_delete(self):
        parser = argparse.ArgumentParser(description='Delete a repository')
        parser.add_argument('name', help='Name of repository')
        args = parser.parse_args(sys.argv[2:])
        repo = Repository(args.name)
        repo.delete()
        print('Repository %s deleted' % repo.name)

    def snapshot_list(self):
        parser = argparse.ArgumentParser(description='List snapshots in a '
                                         'repository')
        parser.add_argument('repo', help='Name of repository')
        args = parser.parse_args(sys.argv[2:])
        repo = Repository(args.repo)
        snapshots = repo.list_snapshots()
        render_template('snapshot_list', repo=repo, snapshots=snapshots)

    def snapshot_details(self):
        parser = argparse.ArgumentParser(description='Show details of a '
                                         'snapshot')
        parser.add_argument('repo', help='Name of repository')
        parser.add_argument('snapshot', help='Name of snapshot')
        args = parser.parse_args(sys.argv[2:])
        repo = Repository(args.repo)
        snapshot = Snapshot(args.snapshot, repo)
        render_template('snapshot_details', snapshot=snapshot)

    def snapshot_create(self):
        parser = argparse.ArgumentParser(description='Create a snapshot')
        parser.add_argument('repo', help='Name of repository')
        parser.add_argument('snapshot', help='Name of snapshot')
        parser.add_argument('-i', '--indices', default='_all',
                            help='Multi-index syntax formatted list of '
                            'indices (Default: _all)')
        parser.add_argument('--ignore-unavailable', action='store_true',
                            help='Allow snapshot creation to continue if an '
                            'index does not exist')
        parser.add_argument('--include-global-state', action='store_false',
                            help='Prevent the inclusion of the cluster global '
                            'state')
        parser.add_argument('--partial', action='store_true', help='Permit '
                            'snapshot creation when not all primary shards '
                            'are available')
        args = parser.parse_args(sys.argv[2:])
        repo = Repository(args.repo)

        ign_unavail = self.__arg_conf(args.ignore_unavailable,
                                      config.get('default',
                                                 'ignore_unavailable'))
        inc_glob_state = self.__arg_conf(args.include_global_state,
                                         config.get('default',
                                                    'include_global_state'))
        partial = self.__arg_conf(args.partial, config.get('default',
                                                           'partial'))

        snapshot = Snapshot(args.snapshot, repo, indices=args.indices,
                            ignore_unavailable=ign_unavail,
                            include_global_state=inc_glob_state,
                            partial=partial)
        print('Snapshot %s created in repository %s' % (snapshot.name,
                                                        repo.name))

    def snapshot_delete(self):
        parser = argparse.ArgumentParser(description='Delete a snapshot')
        parser.add_argument('repo', help='Name of repository')
        parser.add_argument('snapshot', help='Name of snapshot')
        args = parser.parse_args(sys.argv[2:])
        repo = Repository(args.repo)
        snapshot = Snapshot(args.snapshot, repo)
        snapshot.delete()
        print('Snapshot %s deleted from repository %s' % (snapshot.name,
              repo.name))

    def scheduled_backup(self):
        parser = argparse.ArgumentParser(description='Create a backup '
                                         'according to configured schedule')
        parser.add_argument('-t', '--type', default='fs', choices=['fs', 's3',
                            'azure', 'hdfs'], help='Backup type')
        parser.add_argument('-c', '--count', help='Full backup count to '
                            'retain (Default: 4)')
        parser.add_argument('-l', '--life', help='Life time of a backup in '
                            'days before a new full backup will be taken '
                            '(Default: 7)')
        parser.add_argument('-b', '--base-path', help='Base path of backup '
                            'repositories (Default: '
                            '/var/backups/elasticsearch')
        parser.add_argument('-p', '--prefix', help='Backup repository name '
                            'prefix (Default: backup)')
        parser.add_argument('-i', '--indices', default='_all',
                            help='Multi-index syntax formatted list of '
                            'indices (Default: _all)')
        parser.add_argument('--ignore-unavailable', action='store_true',
                            help='Allow snapshot creation to continue if an '
                            'index does not exist')
        parser.add_argument('--include-global-state', action='store_false',
                            help='Prevent the inclusion of the cluster global '
                            'state')
        parser.add_argument('--partial', action='store_true', help='Permit '
                            'snapshot creation when not all primary shards '
                            'are available')
        args = parser.parse_args(sys.argv[2:])
        repo_type = self.__arg_conf(args.type, config.get('backup',
                                                          'backup_type'))
        count = self.__arg_conf(args.count, config.getint('backup',
                                                          'full_backup_count'))
        life = self.__arg_conf(args.life, config.getint('backup',
                                                        'full_backup_life'))
        path = self.__arg_conf(args.base_path, config.get(repo_type,
                                                          'backup_base_path'))
        prefix = self.__arg_conf(args.prefix, config.get('backup', 'prefix'))
        indices = self.__arg_conf(args.indices, config.get('backup',
                                                           'indices'))
        ign_unavail = self.__arg_conf(args.ignore_unavailable,
                                      config.getboolean('default',
                                                        'ignore_unavailable'))
        inc_glob_state = self.__arg_conf(args.include_global_state,
                                         config.getboolean('default',
                                                           'include_global_state'))
        partial = self.__arg_conf(args.partial, config.getboolean('default',
                                                                  'partial'))

        backup = get_backup_repo(repo_type=repo_type, count=count, life=life,
                                 base_path=path, prefix=prefix)
        create_backup(backup, indices=indices, ignore_unavailable=ign_unavail,
                      include_global_state=inc_glob_state, partial=partial)
        remove_old_backups(prefix)


if __name__ == '__main__':
    Commands()
