from datetime import *
from dateutil.parser import *
from es_backup.repository import *
from es_backup.snapshot import *


def get_backup_repo(repo_type, count, life, base_path, prefix):
    repos = list_repos(match=('%s_[0-9]{8}' % prefix))
    repos = sorted(repos, key=lambda repo: repo.name, reverse=True)
    if len(repos) > 0:
        last_backup = parse(repos[0].name.split('_')[-1])
        age = (datetime.today() - last_backup).days
    else:
        age = 99999

    if age > life:
        date = datetime.now().strftime('%Y%m%d')
        repo_name = '%s_%s' % (prefix, date)
        repo_path = '%s/%s' % (base_path, date)
        if repo_type == 'fs':
            backup_repo = FileRepository(repo_name, repo_path)
            print('New backup repo %s created at %s' % (repo_name, repo_path))
            return backup_repo
    else:
        return repos[0]


def create_backup(repo, indices, ignore_unavailable, include_global_state,
                  partial):
    name = datetime.now().strftime('%Y%m%d_%H:%M:%S')
    snapshot = Snapshot(name, repo, indices=indices,
                        ignore_unavailable=ignore_unavailable,
                        include_global_state=include_global_state,
                        partial=partial)
    print('Snapshot %s created in repository %s' % (snapshot.name, repo.name))


def remove_old_backups(prefix):
    full_count = config.getint('backup', 'full_backup_count')
    life = config.getint('backup', 'full_backup_life')
    max_age = full_count * life
    backup_repos = list_repos(match=('%s_[0-9]{8}' % prefix))
    for backup in backup_repos:
        date = parse(backup.name.split('_')[-1])
        age = (datetime.today() - date).days
        if age > max_age:
            print('Aging out backup repository %s' % backup.name)
            backup.delete()
