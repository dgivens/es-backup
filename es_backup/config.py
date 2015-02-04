import ConfigParser
import os
import sys

if os.environ.get('CONFIG') and os.path.exists(os.environ.get('CONFIG')):
    config_path = os.environ['CONFIG']
else:
    cwd = os.path.abspath(os.path.dirname(sys.argv[0]))
    config_path = '%s/etc/es_backup.conf' % cwd

config = ConfigParser.ConfigParser()
config.read(config_path)
