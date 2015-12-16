# std
import glob
import imp
import inspect
import itertools
import logging
import simplejson as json
from os import path

# 3p
from consul import Consul
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

DEFAULT_CONSUL_HOST = '127.0.0.1'
DEFAULT_CONSUL_PORT = 8500
DEFAULT_CONSUL_TOKEN = None
DEFAULT_CONSUL_SCHEME = 'http'
DEFAULT_CONSUL_CONSISTENCY = 'default'
DEFAULT_CONSUL_DATACENTER = None
DEFAULT_CONSUL_VERIFY = True

AUTO_CONF_IMAGES = {
    # image_name: [check_name, class_name]
    'redis': ['redisdb', 'Redis'],
    'nginx': ['nginx', 'Nginx'],
    'consul': ['consul', 'ConsulCheck'],
    'elasticsearch': ['elastic', 'ESCheck'],
}


class KeyNotFound(Exception):
    pass


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

    def __init__(self, agentConfig):
        self.client = None
        self.agentConfig = agentConfig
        self.settings = self._extract_settings(agentConfig)
        self.client = self.get_client()
        self.sd_template_dir = agentConfig.get('sd_template_dir')

    def _extract_settings(self, config):
        raise NotImplementedError()

    def get_client(self, reset=False):
        raise NotImplementedError()

    def client_read(self, path, **kwargs):
        raise NotImplementedError()

    def _get_auto_config(self, image_name):
        for key in AUTO_CONF_IMAGES:
            if key == image_name:
                check_name, class_name = AUTO_CONF_IMAGES[key]
                check = self.get_check_class(check_name, class_name)
                if check is None:
                    log.info("Could not find an auto configuration template for %s."
                             " Leaving it unconfigured." % image_name)
                    return None
                auto_conf = check.get_auto_config()
                # stringify the dict to be consistent with what comes from the config stores
                init_config_tpl = json.dumps(auto_conf.get('init_config'))
                instance_tpl = json.dumps(auto_conf.get('instance'))
                return [check_name, init_config_tpl, instance_tpl]
        return None

    def get_check_class(self, check_name, class_name):
        """Return the class object of a check"""
        from config import get_checksd_path, get_os, PathNotFound

        osname = get_os()
        checks_paths = [glob.glob(path.join(self.agentConfig['additional_checksd'], '*.py'))]
        try:
            checksd_path = get_checksd_path(osname)
            checks_paths.append(glob.glob(path.join(checksd_path, '*.py')))
        except PathNotFound, e:
            log.error(e.args[0])
            return None
        for check in itertools.chain(*checks_paths):
            py_check_name = path.basename(check).split('.')[0]
            if py_check_name == check_name:
                check_module = imp.load_source('checksd_%s' % check_name, check)
                classes = inspect.getmembers(check_module, inspect.isclass)
                for cls_name, clsmember in classes:
                    if cls_name == class_name:
                        return clsmember

    def get_check_tpl(self, image, **kwargs):
        """Retrieve template config strings from the ConfigStore."""
        try:
            # Try to read from the user-supplied config
            check_name = self.client_read(path.join(self.sd_template_dir, image, 'check_name').lstrip('/'))
            init_config_tpl = self.client_read(path.join(self.sd_template_dir, image, 'init_config').lstrip('/'))
            instance_tpl = self.client_read(path.join(self.sd_template_dir, image, 'instance').lstrip('/'))
        except (KeyNotFound, TimeoutError):
            # If it failed, try to read from auto-config templates
            log.info("Could not find directory {0} in the config store, "
                     "trying to auto-configure the check...".format(image))
            auto_config = self._get_auto_config(image)
            if auto_config is not None:
                check_name, init_config_tpl, instance_tpl = auto_config
            else:
                return None
        except Exception:
            log.info(
                'Fetching the value for {0} in the config store failed, '
                'this check will not be configured by the service discovery.'.format(image))
            return None
        template = [check_name, init_config_tpl, instance_tpl]
        return template

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
    def _extract_settings(self, config):
        """Extract settings from a config object"""
        settings = {
            'host': config.get('sd_backend_host', DEFAULT_ETCD_HOST),
            'port': int(config.get('sd_backend_port', DEFAULT_ETCD_PORT)),
            # these two are always set to their default value for now
            'allow_reconnect': config.get('etcd_allow_reconnect', DEFAULT_RECO),
            'protocol': config.get('etcd_protocol', DEFAULT_ETCD_PROTOCOL),
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

    def client_read(self, path, **kwargs):
        """Retrieve a value from a etcd key."""
        try:
            return self.client.read(path, timeout=kwargs.get('timeout', DEFAULT_TIMEOUT)).value
        except EtcdKeyNotFound:
            raise KeyNotFound("The key %s was not found in etcd" % path)
        except TimeoutError, e:
            raise e


class ConsulStore(ConfigStore):
    """Implementation of a config store client for consul"""
    def _extract_settings(self, config):
        """Extract settings from a config object"""
        settings = {
            'host': config.get('sd_backend_host', DEFAULT_CONSUL_HOST),
            'port': int(config.get('sd_backend_port', DEFAULT_CONSUL_PORT)),
            # all these are set to their default value for now
            'token': config.get('consul_token', None),
            'scheme': config.get('consul_scheme', DEFAULT_CONSUL_SCHEME),
            'consistency': config.get('consul_consistency', DEFAULT_CONSUL_CONSISTENCY),
            'verify': config.get('consul_verify', DEFAULT_CONSUL_VERIFY),
        }
        return settings

    def get_client(self, reset=False):
        """Return a consul client, create it if needed"""
        if self.client is None or reset is True:
            self.client = Consul(
                host=self.settings.get('host'),
                port=self.settings.get('port'),
                token=self.settings.get('token'),
                scheme=self.settings.get('scheme'),
                consistency=self.settings.get('consistency'),
                verify=self.settings.get('verify'),
            )
        return self.client

    def client_read(self, path, **kwargs):
        """Retrieve a value from a consul key."""
        res = self.client.kv.get(path)[1]
        if res is not None:
            return res.get('Value')
        else:
            raise KeyNotFound("The key %s was not found in consul" % path)
