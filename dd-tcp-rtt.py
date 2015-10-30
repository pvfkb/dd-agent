# set up logging before importing any other components
CHECKNAME = 'dd-tcp-rtt'

if __name__ == '__main__':
    from config import initialize_logging  # noqa
    initialize_logging(CHECKNAME)

# std
import logging
import os
import signal
import sys
from util import yLoader

# 3rd party
import yaml

# datadog
from config import (
    get_confd_path,
    get_config,
    get_logging_config,
    PathNotFound,
)
from utils.platform import Platform
from utils.subprocess_output import subprocess


log = logging.getLogger(CHECKNAME)


class DDTcpRtt(object):
    """
    Start dd_tcp-rtt if configured
    """
    def __init__(self, confd_path, agentConfig, bin_path):
        self.confd_path = confd_path
        self.agentConfig = agentConfig
        self.logging_config = get_logging_config()
        self.bin_path = bin_path

    def terminate(self):
        self.dd_tcp_rtt.terminate()

    def _handle_sigterm(self, signum, frame):
        # Terminate jmx process on SIGTERM signal
        log.debug("Caught sigterm. Stopping subprocess.")
        self.dd_tcp_rtt.terminate()

    def register_signal_handlers(self):
        """
        Enable SIGTERM and SIGINT handlers
        """
        try:
            # Gracefully exit on sigterm
            signal.signal(signal.SIGTERM, self._handle_sigterm)

            # Handle Keyboard Interrupt
            signal.signal(signal.SIGINT, self._handle_sigterm)

        except ValueError:
            log.exception("Unable to register signal handlers.")

    def get_configuration(self, clean_status_file=True):
        """
        Instantiate DD-TCP-RTT parameters.
        """
        conf = os.path.join(self.confd_path, '{filename}.yaml'.format(filename=CHECKNAME))
        filename = os.path.basename(conf)
        check_name = filename.split('.')[0]

        if os.path.exists(conf) and check_name == CHECKNAME:
            f = open(conf)
            try:
                check_config = yaml.load(f.read(), Loader=yLoader)
                assert check_config is not None
                f.close()
            except Exception:
                f.close()
                log.error("Unable to parse yaml config in %s" % conf)
                return ''

        # configuration is valid yaml - good enough to parse
        return conf


    def run(self, redirect_std_streams=False):
        """
        Run DDTcpRtt

        redirect_std_streams: if left to False, the stdout and stderr of DDTcpRtt are streamed
        directly to the environment's stdout and stderr and cannot be retrieved via python's
        sys.stdout and sys.stderr. Set to True to redirect these streams to python's sys.stdout
        and sys.stderr.
        """
        try:
            config = self.get_configuration()
            if not config:
                raise

            return self._start(config, redirect_std_streams)
        except Exception:
            log.exception("Error while initiating DDTcpRtt")
            raise

    def _start(self, config_file, redirect_std_streams):
        log.info("Starting dd-tcp-rtt:")
        try:

            subprocess_args = [
                self.bin_path,  # Path to the java bin
                '-cfg={config}'.format(config=config_file),
            ]

            if Platform.is_windows():
                #probably won't work on windows.
                return

            log.info("Running %s" % " ".join(subprocess_args))

            # Launch dd-tcp-rtt subprocess
            dd_tcp_rtt = subprocess.Popen(
                subprocess_args,
                close_fds=not redirect_std_streams,  # set to True instead of False when the streams are redirected for WIN compatibility
                stdout=subprocess.PIPE if redirect_std_streams else None,
                stderr=subprocess.PIPE if redirect_std_streams else None
            )
            self.dd_tcp_rtt = dd_tcp_rtt

            # Register SIGINT and SIGTERM signal handlers
            self.register_signal_handlers()

            if redirect_std_streams:
                # Wait for DDTcpRtt to return, and write out the stdout and stderr of DDTcpRtt to sys.stdout and sys.stderr
                out, err = dd_tcp_rtt.communicate()
                sys.stdout.write(out)
                sys.stderr.write(err)
            else:
                # Wait for DDTcpRtt to return
                dd_tcp_rtt.wait()

            return dd_tcp_rtt.returncode

        except OSError:
            bin_path_msg = "Couldn't launch dd-tcp-rtt. Is binary in your PATH?"
            log.exception(bin_path_msg)
            raise
        except Exception:
            log.exception("Couldn't launch dd-tcp-rtt")
            raise


def init(config_path=None):
    agentConfig = get_config(parse_args=False, cfg_path=config_path)
    try:
        confd_path = get_confd_path()
    except PathNotFound, e:
        log.error("No conf.d folder found at '%s' or in the directory where"
                  "the Agent is currently deployed.\n" % e.args[0])

    return confd_path, agentConfig


def main(config_path=None):
    """ DD-TCP-RTT main entry point """
    confd_path, agentConfig = init(config_path)

    tcprtt = DDTcpRtt(confd_path, agentConfig)
    return tcprtt.run()

if __name__ == '__main__':
    sys.exit(main())
