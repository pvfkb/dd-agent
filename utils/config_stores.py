# std
import logging
from os import path

# project
from tests.checks.common import get_check_class

# 3p
from etcd import Client as etcd_client
from urllib3.exceptions import TimeoutError

log = logging.getLogger(__name__)

DEFAULT_ETCD_HOST = '127.0.0.1'
DEFAULT_ETCD_PORT = 4001
DEFAULT_ETCD_PROTOCOL = 'http'
DEFAULT_RECO = True
DEFAULT_TIMEOUT = 5
SD_TEMPLATE_DIR = '/datadog/check_configs'

AUTO_CONF_IMAGES = {
    # image_name: check_name
    'redis': 'redisdb',
    'nginx': 'nginx',
    'mongo': 'mongo',
    'consul': 'consul',
    'elasticsearch': 'elastic',
}


class ConfigStore:
    """Singleton for config stores"""
    _instance = None

    def __init__(self, store=None, config=None):
        if self._instance is None:
            if store == 'etcd':
                self._instance = EtcdStore(config)
            elif store == 'consul':
                self._instance = ConsulStore(config)
        else:
            return self._instance

    def __get_attr__(self, name):
        return getattr(self._instance, name)

    def __set_attr__(self, name, value):
        return setattr(self._instance, name, value)

    @staticmethod
    def extract_sd_config(config):
        """Extract configuration about service discovery for the agent"""
        sd_config = {}
        if config.has_option('Main', 'sd_config_backend'):
            sd_config['sd_config_backend'] = config.get('Main', 'sd_config_backend')
        else:
            sd_config['sd_config_backend'] = 'etcd'
        if config.has_option('Main', 'backend_template_dir'):
            sd_config['sd_template_dir'] = config.get(
                'Main', 'backend_template_dir')
        else:
            sd_config['sd_template_dir'] = SD_TEMPLATE_DIR
        if config.has_option('Main', 'sd_backend_host'):
            sd_config['sd_backend_host'] = config.get(
                'Main', 'sd_backend_host')
        if config.has_option('Main', 'sd_backend_port'):
            sd_config['sd_backend_port'] = config.get(
                'Main', 'sd_backend_port')
        return sd_config


class ConfigStoreClient:
    """Mother class for the configuration store clients"""

    def __init__(self, config):
        self.client = None
        self.settings = self._extract_settings(config)
        self.client = self.get_client()
        self.sd_template_dir = config.get('sd_template_dir')

    def extract_settings(self, config):
        raise NotImplementedError()

    def get_client(self, reset=False):
        raise NotImplementedError()

    def get_check_tpl(self, key, **kwargs):
        raise NotImplementedError()

    def set_client_config(self, config):
        self.settings = self._extract_settings(config)
        self.client = self.get_client(reset=True)

    def get_check_object(self, image_name):
        for key in AUTO_CONF_IMAGES:
            if key == image_name:
                check_name = AUTO_CONF_IMAGES[key]
                check = get_check_class(check_name)
                auto_conf = check.get_auto_config()
                init_config_tpl = auto_conf.get('init_config')
                instance_tpl = auto_conf.get('instance')
                return [check_name, init_config_tpl, instance_tpl]
        return None


class EtcdStore(ConfigStoreClient):
    """Implementation of a config store client for etcd"""

    def extract_settings(self, config):
        """Extract settings from a config object"""
        self.settings = {
            'host': config.get('host', DEFAULT_ETCD_HOST),
            'port': int(config.get('port', DEFAULT_ETCD_PORT)),
            'allow_reconnect': config.get('allow_reconnect', DEFAULT_RECO),
            'protocol': config.get('protocol', DEFAULT_ETCD_PROTOCOL),
        }

    def get_client(self, reset=False):
        if self.client is None or reset is True:
            self.client = etcd_client(self.settings)
        return self.client

    def get_check_tpl(self, image, **kwargs):
        """Retrieve template config strings from etcd."""
        try:
            # Try to read from the user-supplied config
            check_name = self.client.read(path.join(self.sd_template_dir, image, 'check_name')).value
            init_config_tpl = self.client.read(
                path.join(self.sd_template_dir, image, 'init_config'),
                timeout=kwargs.get('timeout', DEFAULT_TIMEOUT)).value
            instance_tpl = self.client.read(
                path.join(self.sd_template_dir, image, 'instance'),
                timeout=kwargs.get('timeout', DEFAULT_TIMEOUT)).value
        except (KeyError, TimeoutError):
            # If it failed, try to read from auto-config templates
            log.info("Could not find directory {0} in etcd configs, "
                     "trying to auto-configure the check...".format(image))
            auto_config = self.get_auto_config(image)
            if auto_config is not None:
                check_name, init_config_tpl, instance_tpl = auto_config
        except Exception:
            log.info(
                'Fetching the value for {0} in etcd failed, '
                'this check will not be configured by the service discovery.'.format(image))
            return None
        template = [check_name, init_config_tpl, instance_tpl]
        return template


class ConsulStore(ConfigStoreClient):
    """Implementation of a config store client for consul"""
