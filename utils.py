import argparse
import os
import yaml
import logging


VERIABLES = ['VERSION']


def get_test_config():
    config = get_config()
    config['DB_NAME'] = 'test_database'
    config['PROD'] = False
    config['HOST'] = None
    return config


def load_config(file_path):
    with open(file_path, 'r') as conf_file:
        config = yaml.load(conf_file)
    return config


def get_config():
    log = logging.getLogger()
    config = {}
    try:
        config = load_config(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'config.yaml'))
    except FileNotFoundError:
        pass
    except yaml.YAMLError:
        log.error('Failed to load config, incorrect format. Trying to load from ENV variables')

    config.update(_load_from_env(VERIABLES))
    return config


def _load_from_env(keys):
    env_config = {}

    for key in keys:
        env_value = os.environ.get(key)

        if env_value:
            env_config[key] = env_value

    return env_config