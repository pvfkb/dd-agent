# Core modules
import os
import re
import logging
import platform
import subprocess
import sys
import time
import datetime
import socket

# Needed to identify server uniquely
import uuid
try:
    from hashlib import md5
except ImportError: # Python < 2.5
    from md5 import new as md5

from checks.nagios import Nagios
from checks.build import Hudson

from checks.db.mysql import MySql
from checks.db.mongo import MongoDb
from checks.db.redisDb import Redis
from checks.db.couch import CouchDb

from checks.queue import RabbitMq
from checks.system import Disk, IO, Load, Memory, Network, Processes, Cpu
from checks.web import Apache, Nginx
from checks.ganglia import Ganglia
from checks.datadog import RollupLP as ddRollupLP
from checks.cassandra import Cassandra

from resources.processes import Processes as ResProcesses
from resources.mockup_rails import RailsMockup

def recordsize(func):
    "Record the size of the response"
    def wrapper(*args, **kwargs):
        logger = logging.getLogger("checks")
        res = func(*args, **kwargs)
        logger.debug("SIZE: {0} wrote {1} bytes uncompressed".format(func, len(str(res))))
        return res
    return wrapper

class checks:
    def __init__(self, agentConfig, rawConfig, emitter):
        self.agentConfig = agentConfig
        self.rawConfig = rawConfig
        self.plugins = None
        self.emitter = emitter
        
        macV = None
        if sys.platform == 'darwin':
            macV = platform.mac_ver()
        
        # Output from top is slightly modified on OS X 10.6 (case #28239)
        if macV and macV[0].startswith('10.6.'):
            self.topIndex = 6
        else:
            self.topIndex = 5
    
        self.os = None
        
        self.checksLogger = logging.getLogger('checks')
        # Set global timeout to 15 seconds for all sockets (case 31033). Should be long enough
        import socket
        socket.setdefaulttimeout(15)
        
        self.linuxProcFsLocation = self.getMountedLinuxProcFsLocation()
        
        self._apache = Apache()
        self._nginx = Nginx()
        self._disk = Disk()
        self._io = IO()
        self._load = Load(self.linuxProcFsLocation)
        self._memory = Memory(self.linuxProcFsLocation, self.topIndex)
        self._network = Network()
        self._processes = Processes()
        self._cpu = Cpu()
        self._couchdb = CouchDb(self.checksLogger)
        self._mongodb = MongoDb(self.checksLogger)
        self._mysql = MySql(self.checksLogger)
        self._rabbitmq = RabbitMq()
        self._ganglia = Ganglia()
        self._cassandra = Cassandra()
        self._redis = Redis(self.checksLogger)

        if agentConfig.get('has_datadog',False):
            self._datadogs = [ddRollupLP()]
        else:
            self._datadogs = None

        self._event_checks = [Hudson(), Nagios(socket.gethostname())]
        self._resources_checks = [ResProcesses(self.checksLogger,self.agentConfig)]
 
    #
    # Checks - FIXME migrating to the new Check interface is a WIP
    #
    @recordsize 
    def getApacheStatus(self):
        return self._apache.check(self.checksLogger, self.agentConfig)

    @recordsize 
    def getCouchDBStatus(self):
        return self._couchdb.check(self.agentConfig)
    
    @recordsize
    def getDiskUsage(self):
        return self._disk.check(self.checksLogger, self.agentConfig)

    @recordsize
    def getIOStats(self):
        return self._io.check(self.checksLogger, self.agentConfig)
            
    @recordsize
    def getLoadAvrgs(self):
        return self._load.check(self.checksLogger, self.agentConfig)

    @recordsize 
    def getMemoryUsage(self):
        return self._memory.check(self.checksLogger, self.agentConfig)
        
    @recordsize     
    def getMongoDBStatus(self):
        return self._mongodb.check(self.agentConfig)

    @recordsize
    def getMySQLStatus(self):
        return self._mysql.check(self.agentConfig)
        
    @recordsize
    def getNetworkTraffic(self):
        return self._network.check(self.checksLogger, self.agentConfig)
    
    @recordsize
    def getNginxStatus(self):
        return self._nginx.check(self.checksLogger, self.agentConfig)
        
    @recordsize
    def getProcesses(self):
        return self._processes.check(self.checksLogger, self.agentConfig)
        
    @recordsize
    def getRabbitMQStatus(self):
        return self._rabbitmq.check(self.checksLogger, self.agentConfig)

    @recordsize
    def getGangliaData(self):
        return self._ganglia.check(self.checksLogger, self.agentConfig)

    @recordsize
    def getDatadogData(self):
        result = {}
        if self._datadogs is not None:
            for dd in self._datadogs:
                result[dd.key] = dd.check(self.checksLogger, self.agentConfig)

        return result
        
    @recordsize
    def getCassandraData(self):
        return self._cassandra.check(self.checksLogger, self.agentConfig)

    @recordsize
    def getRedisData(self):
        return self._redis.check(self.agentConfig)

    #
    # CPU Stats
    #
    @recordsize
    def getCPUStats(self):
        return self._cpu.check(self.checksLogger, self.agentConfig)

    #
    # Postback
    #
    def doChecks(self, sc, firstRun, systemStats=False):
        # Do the checks
        apacheStatus = self.getApacheStatus()
        diskUsage = self.getDiskUsage()
        loadAvrgs = self.getLoadAvrgs()
        memory = self.getMemoryUsage()
        mysqlStatus = self.getMySQLStatus()
        networkTraffic = self.getNetworkTraffic()
        nginxStatus = self.getNginxStatus()
        processes = self.getProcesses()
        rabbitmq = self.getRabbitMQStatus()
        mongodb = self.getMongoDBStatus()
        couchdb = self.getCouchDBStatus()
        ioStats = self.getIOStats()
        cpuStats = self.getCPUStats()
        gangliaData = self.getGangliaData()
        datadogData = self.getDatadogData()
        cassandraData = self.getCassandraData()
        redisData = self.getRedisData()
 
        checksData = {
            'collection_timestamp': time.time(),
            'os' : self.os, 
            'agentKey' : self.agentConfig['agentKey'], 
            'agentVersion' : self.agentConfig['version'], 
            'diskUsage' : diskUsage, 
            'loadAvrg1' : loadAvrgs['1'], 
            'loadAvrg5' : loadAvrgs['5'], 
            'loadAvrg15' : loadAvrgs['15'], 
            'memPhysUsed' : memory['physUsed'], 
            'memPhysFree' : memory['physFree'], 
            'memSwapUsed' : memory['swapUsed'], 
            'memSwapFree' : memory['swapFree'], 
            'memCached' : memory['cached'], 
            'networkTraffic' : networkTraffic, 
            'processes' : processes,
            'apiKey': self.agentConfig['apiKey'],
            'events': {},
        }

        if cpuStats is not False and cpuStats is not None:
            checksData.update(cpuStats)

        if gangliaData is not False and gangliaData is not None:
            checksData['ganglia'] = gangliaData
           
        if datadogData is not False and datadogData is not None:
            checksData['datadog'] = datadogData
            
        if cassandraData is not False and cassandraData is not None:
            checksData['cassandra'] = cassandraData
 
        # Apache Status
        if apacheStatus != False:           
            checksData['apacheReqPerSec'] = apacheStatus['reqPerSec']
            checksData['apacheBusyWorkers'] = apacheStatus['busyWorkers']
            checksData['apacheIdleWorkers'] = apacheStatus['idleWorkers']
            
        # MySQL Status
        if mysqlStatus != False:
            
            
            checksData['mysqlConnections'] = mysqlStatus['connections']
            checksData['mysqlCreatedTmpDiskTables'] = mysqlStatus['createdTmpDiskTables']
            checksData['mysqlMaxUsedConnections'] = mysqlStatus['maxUsedConnections']
            checksData['mysqlOpenFiles'] = mysqlStatus['openFiles']
            checksData['mysqlSlowQueries'] = mysqlStatus['slowQueries']
            checksData['mysqlTableLocksWaited'] = mysqlStatus['tableLocksWaited']
            checksData['mysqlThreadsConnected'] = mysqlStatus['threadsConnected']
            
            if mysqlStatus['secondsBehindMaster'] != None:
                checksData['mysqlSecondsBehindMaster'] = mysqlStatus['secondsBehindMaster']
        
        # Nginx Status
        if nginxStatus:
            checksData['nginxConnections'] = nginxStatus['connections']
            checksData['nginxReqPerSec'] = nginxStatus['reqPerSec']
            
        # RabbitMQ
        if rabbitmq:
            checksData['rabbitMQ'] = rabbitmq
        
        # MongoDB
        if mongodb:
            checksData['mongoDB'] = mongodb
            
        # CouchDB
        if couchdb:
            checksData['couchDB'] = couchdb
        
        if ioStats:
            checksData['ioStats'] = ioStats
            
        if redisData:
            # Redis data already has the proper metric names
            checksData.update(redisData)
        
       # Include system stats on first postback
        if firstRun == True:
            checksData['systemStats'] = systemStats
            
        # Include server indentifiers
        try:
            checksData['internalHostname'] = socket.gethostname()
        except socket.error:
            self.checksLogger.exception('Unable to get hostname')
        
        # Generate a unique name that will stay constant between
        # invocations, such as platform.node() + uuid.getnode()
        # Use uuid5, which does not depend on the clock and is
        # recommended over uuid3.
        # This is important to be able to identify a server even if
        # its drives have been wiped clean.
        # Note that this is not foolproof but we can reconcile servers
        # on the back-end if need be, based on mac addresses.
        checksData['uuid'] = uuid.uuid5(uuid.NAMESPACE_DNS, platform.node() + str(uuid.getnode())).hex
        self.checksLogger.debug('doChecks: added uuid %s' % checksData['uuid'])
        
        # Process the event checks. 
        for event_check in self._event_checks:
            event_data = event_check.check(self.checksLogger, self.agentConfig)
            if event_data:
                checksData['events'][event_check.key] = event_data
       
        if firstRun:
            checksData['events']['System'] = [{'api_key': self.agentConfig['apiKey'],
                                              'host': checksData['internalHostname'],
                                              'timestamp': int(time.mktime(datetime.datetime.now().timetuple())),
                                              'event_type':'agent startup',
                                            }]
        
        self.emitter(checksData, self.checksLogger, self.agentConfig)
        
        sc.enter(self.agentConfig['checkFreq'], 1, self.doChecks, (sc, False))  
        
    def getMountedLinuxProcFsLocation(self):
        self.checksLogger.debug('getMountedLinuxProcFsLocation: attempting to fetch mounted partitions')
        
        # Lets check if the Linux like style procfs is mounted
        mountedPartitions = subprocess.Popen(['mount'], stdout = subprocess.PIPE, close_fds = True).communicate()[0]
        location = re.search(r'linprocfs on (.*?) \(.*?\)', mountedPartitions)
        
        # Linux like procfs file system is not mounted so we return False, else we return mount point location
        if location == None:
            return False

        location = location.group(1)
        return location
