# std
import logging
import re

# project
from utils.service_discovery.docker_backend import SDDockerBackend

log = logging.getLogger(__name__)


class ServiceDiscoveryBackend:
    """Singleton for service discovery backends"""
    _instance = None

    def __init__(self, backend=None, agentConfig=None):
        if self._instance is None:
            if backend == 'docker':
                self._instance = SDDockerBackend(agentConfig)
        else:
            return self._instance

    def __get_attr__(self, name):
        return getattr(self._instance, name)

    def __set__attr(self, name, value):
        return setattr(self._instance, name, value)


class SDGenericBackend:
    """Mother class for service discovery backends"""
    def __init__(self, agentConfig):
        self.PLACEHOLDER_REGEX = re.compile(r'%%.+?%%')
        self.agentConfig = agentConfig

    def get_configs(self):
        """Get the config for all docker containers running on the host."""
        raise NotImplementedError()

    def _render_template(self, init_config_tpl, instance_tpl, variables):
        """Replace placeholders in a template with the proper values.
           Return a list made of `init_config` and `instances`."""
        config = [init_config_tpl, instance_tpl]
        for tpl in config:
            for key in tpl:
                for var in self.PLACEHOLDER_REGEX.findall(str(tpl[key])):
                    if var.strip('%') in variables and variables[var.strip('%')]:
                        tpl[key] = tpl[key].replace(var, variables[var.strip('%')])
                    else:
                        log.warning('Failed to find a value for the {0} parameter.'
                                    ' The check might not be configured properly.'.format(key))
                        tpl[key].replace(var, '')
        config[1] = config[1]
        return config
