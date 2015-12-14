# std
import logging
import simplejson as json
from os import path

# 3p
from etcd import EtcdKeyNotFound
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


class ConfigStore(object):
    """Singleton for config stores"""
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            agentConfig = kwargs.get('agentConfig', {})
            if agentConfig.get('sd_config_backend') == 'etcd':
                cls._instance = object.__new__(EtcdStore, agentConfig)
            elif agentConfig.get('sd_config_backend') == 'consul':
                cls._instance = object.__new__(ConsulStore, agentConfig)
        return cls._instance

    def _extract_settings(self, config):
        raise NotImplementedError()

    def get_client(self, reset=False):
        raise NotImplementedError()

    def get_check_tpl(self, key, **kwargs):
        raise NotImplementedError()

    def set_client_config(self, config):
        self.settings = self._extract_settings(config)
        self.client = self.get_client(reset=True)

    def _get_auto_config(self, image_name):
        from tests.checks.common import get_check_class
        for key in AUTO_CONF_IMAGES:
            if key == image_name:
                check_name = AUTO_CONF_IMAGES[key]
                check = get_check_class(check_name)
                auto_conf = check.get_auto_config()
                # stringify the dict to be consistent with what comes from the config stores
                init_config_tpl = json.dumps(auto_conf.get('init_config'))
                instance_tpl = json.dumps(auto_conf.get('instance'))
                return [check_name, init_config_tpl, instance_tpl]
        return None

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


class EtcdStore(ConfigStore):
    """Implementation of a config store client for etcd"""
    def __init__(self, agentConfig):
        self.client = None
        self.settings = self._extract_settings(agentConfig)
        self.client = self.get_client()
        self.sd_template_dir = agentConfig.get('sd_template_dir')

    def _extract_settings(self, config):
        """Extract settings from a config object"""
        settings = {
            'host': config.get('sd_backend_host', DEFAULT_ETCD_HOST),
            'port': int(config.get('sd_backend_port', DEFAULT_ETCD_PORT)),
            'allow_reconnect': config.get('allow_reconnect', DEFAULT_RECO),
            'protocol': config.get('protocol', DEFAULT_ETCD_PROTOCOL),
        }
        return settings

    def get_client(self, reset=False):
        if self.client is None or reset is True:
            self.client = etcd_client(
                host=self.settings.get('host'),
                port=self.settings.get('port'),
                allow_reconnect=self.settings.get('allow_reconnect'),
                protocol=self.settings.get('protocol'),
            )
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
        except (EtcdKeyNotFound, TimeoutError):
            # If it failed, try to read from auto-config templates
            log.info("Could not find directory {0} in etcd configs, "
                     "trying to auto-configure the check...".format(image))
            auto_config = self._get_auto_config(image)
            if auto_config is not None:
                check_name, init_config_tpl, instance_tpl = auto_config
        except Exception:
            log.info(
                'Fetching the value for {0} in etcd failed, '
                'this check will not be configured by the service discovery.'.format(image))
            return None
        template = [check_name, init_config_tpl, instance_tpl]
        return template


class ConsulStore(ConfigStore):
    """Implementation of a config store client for consul"""
