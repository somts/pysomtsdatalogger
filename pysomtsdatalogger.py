#!/usr/bin/env python
'''
pysomtsdatalogger.py
    Python-based network/serial raw data consumer and logger via a
    RabbitMQ AMQP producer/consumer model.
    https://github.com/somts/pysomtsdatalogger

Package installation:
CentOS: requires EPEL and pip to work. Commands:
  install base packages with: yum -y install PyYAML epel-release pyserial
  install EPEL packages with: yum -y install python-pip
  install PyPi packages with : pip install pika voluptuous

Ubuntu commands:
    sudo apt install python-celery python-pip python-serial python-voluptuous
    sudo pip install PyYAML

OS X: requires pip to work. Commands:
  PyPi packages: pip install PyYAML pika pyserial voluptuous
  NOTE: OS X's network stack doesn't work with <broadcast> magic address
'''
import logging,logging.handlers,multiprocessing,os,sys,time
import pika,serial,socket,yaml
from argparse import ArgumentParser
from celery import Celery
from datetime import datetime
from voluptuous import Schema

class PySOMTSDataLogger:
    '''Log raw data'''

    loglevel = logging.INFO

    schema_main = Schema({
        'basedirectory': str,
        'logfile': str,
        'perdaylogfiles': bool,
        'prefixlogfiles': bool,
        })
    default_inputinet_values = {
            'ip':'',
            'port':0,
            'bufsize':4096,
            'proto':'udp',
            'family':'inet',
            }
    schema_producer_inputinet = Schema({
        'ip':str,
        'port':int,
        'bufsize':int,
        'proto':str,
        'broadcast':bool,
        'family':str,
        })
    default_inputserial_values = {
            'port':'',
            'baudrate':19200,
            'bytesize':8,
            'parity':'N',
            'stopbits':1,
            'timeout':1000,
            'xonxoff':False,
            'rtscts':False,
            'write_timeout':1000,
            'dsrdtr':False,
            'inter_byte_timeout':0,
            }
    schema_producer_inputserial = Schema({
        'port':str,
        'baudrate':int,
        'bytesize':int,
        'parity':str,
        'stopbits':int,
        'timeout':int,
        'xonxoff':bool,
        'rtscts':bool,
        'write_timeout':int,
        'dsrdtr':bool,
        'inter_byte_timeout':int
        })
    schema_consumer = Schema({
        'amqphost':str,
        'amqpport':int,
        'comments':str,
        'dataformat':str,
        'description':str,
        'directory':str,
        'inop':bool,
        'inopreason':str,
        'producer': dict,
        })
    default_producer_values = {
            'addtimestamp':False,
            'asciidata':False,
            'amqphost':'127.0.0.1',
            'amqpport':5672,
            'buffersize':1024,
            'comments':'',
            'dataformat':'',
            'description':'',
            'directory':'',
            'inop':False,
            'inopreason':'',
            'inputserial':dict(),
            'inputinet':dict(),
            'manufacturer':'',
            'measures':list(),
            'model':'',
            'nooutputfiles':False,
            'outputtcpport':'',
            'outputudpbroadcast':False,
            'outputudpport':'',
            'perdaylogfiles':True,
            'recordlength':0,
            'reformat':'',
            'selfconsume':False,
            'serialnumber':'',
            'survey_date':'',
            'tcpport':0,
            'timeoutms':1000,
            'timestampformat':"%s.%f\t",
            }
    schema_producer = Schema({
        'amqphost':str,
        'amqpport':int,
        'addtimestamp':bool,
        'asciidata':bool,
        'buffersize':int,
        'comments':str,
        'dataformat':str,
        'description':str,
        'directory':str,
        'inop':bool,
        'inopreason':str,
        'inputserial': dict,
        'inputinet':dict,
        'manufacturer':str,
        'measures':list,
        'model':str,
        'nooutputfiles':bool,
        'outputtcpport':str,
        'outputudpbroadcast':bool,
        'outputudpport':str,
        'perdaylogfiles':bool,
        'recordlength':int,
        'reformat':str,
        'selfconsume':bool,
        'serialnumber':str,
        'survey_date':str,
        'tcpport':int,
        'timeoutms':int,
        'timestampformat':str,
        })

    def __init__(self,
            logformat='[%(levelname)s] %(asctime)s %(lineno)d %(message)s'):

        self.jobs = []
        self.jobdescriptions = []
        self.amqphostports   = []
        self.amqpconnections = {}
        self.amqpchannels    = {}

        # Parse the command-line arguments
        self.parseargs()

        # Set up logging
        logging.basicConfig(format=logformat,level=self.loglevel)
        self.logger = logging.getLogger()

        # Parse and process our config
        self.parseandprocessyaml()

        # Start listening processes
        self.queueproducers()
        self.queueconsumers()
        self.connectamqp()
        self.start()

    def connectamqp(self):
        '''Per https://www.rabbitmq.com/tutorials/amqp-concepts.html,
        we should establish a single TCP connection to each AMQP daemon
        '''
        for i in self.amqphostports:
            myhost = '%s:%i' % i
            # Prime a channel counter for this host
            self.amqpchannels.update({ myhost : None })

            self.logger.info('Establishing AMQP connection to %s' % myhost)

            # Maintain a dict of our connections
            self.amqpconnections.update({
                myhost : pika.BlockingConnection(
                pika.ConnectionParameters(host=i[0],port=i[1]))
                })

    def queueconsumers(self):
        '''make a new job for each consumer'''
        for k,v in self.conf['consumers'].items():
            return

    def queueproducers(self):
        '''make a new job for each producer'''
        for k,v in self.conf['producers'].items():
            if v['inop'] == True:
                if '' == 'inopreason':
                    raise ValueError(
                            'module %s set to INOP, but no reason given' % k)
                self.logger.critical(
                        'PID %s, ' % os.getpid() +
                        'Not running module "%s" because set to ' % k +
                        'INOP for reason "%s"' % v['inopreason']
                        )
                continue

            elif v['inputinet']:
                mytarget = self.inetproduce
                mydesc = '%s/%s:%i' % (
                        v['inputinet']['proto'].upper(),
                        v['inputinet']['ip'],
                        v['inputinet']['port'],
                        )

            elif v['inputserial']:
                mytarget = self.serialproduce
                mydesc = 'Serial port %s, %s baud' % \
                        (inputserial['port'],inputserial['baudrate'])

            else:
                continue

            # If we got here, add a job to our queue
            mykwargs = v.copy()
            mykwargs['name'] = k
            self.jobs.append(multiprocessing.Process(
                target=mytarget,
                name='producer-%s' % k,
                kwargs=mykwargs,
                ))
            self.jobdescriptions.append(mydesc)

    def start(self):
        '''Kick off queued jobs and wait around'''

        for i,p in enumerate(self.jobs):
            p.start()
            self.logger.info('PID %i, started %s using %s' % \
                    (p.pid,p.name,self.jobdescriptions[i]))

        # Wait around for an interrupt.
        while True:
            try:
                time.sleep(10)
                for i,p in enumerate(self.jobs):
                    self.logger.info(
                            'Checking if %s, using %s, is alive: %s' % (
                                p.name,self.jobdescriptions[i],p.is_alive()))
                time.sleep(890)
            except (KeyboardInterrupt,SystemExit):
                self.logger.critical('%s, so quitting...' % sys.exc_info()[0])
                for i,p in enumerate(self.jobs):
                    self.logger.critical('PID %i, stopping %s using %s...' % \
                            (p.pid,p.name,self.jobdescriptions[i]))
                    p.terminate()
                self.logger.critical('All stop.')
                sys.exit()

    def serialproduce(self,
            name,
            amqphost,
            amqpport,
            addtimestamp,
            asciidata,
            buffersize,
            comments,
            dataformat,
            description,
            directory,
            inop,
            inopreason,
            inputserial,
            manufacturer,
            measures,
            model,
            nooutputfiles,
            outputtcpport,
            outputudpbroadcast,
            outputudpport,
            perdaylogfiles,
            recordlength,
            reformat,
            selfconsume,
            serialnumber,
            survey_date,
            tcpport,
            timeoutms,
            timestampformat,
            ):
        '''Open a serial port and listen for data indefinitely'''
        if not inputserial:
            raise ValueError('Did not get dict() for inputserial')
        return

    def inetproduce(self,
            name,
            amqphost,
            amqpport,
            addtimestamp,
            asciidata,
            buffersize,
            comments,
            dataformat,
            description,
            directory,
            inop,
            inopreason,
            inputinet,
            manufacturer,
            measures,
            model,
            nooutputfiles,
            outputtcpport,
            outputudpbroadcast,
            outputudpport,
            perdaylogfiles,
            recordlength,
            reformat,
            selfconsume,
            serialnumber,
            survey_date,
            tcpport,
            timeoutms,
            timestampformat,
            ):
        '''Open a network socket and listen for data indefinitely'''


        if not inputinet:
            raise ValueError('Did not get dict() for inputinet')

        if inputinet['proto'] == 'tcp':
            socktype = socket.SOCK_STREAM
        elif inputinet['proto'] == 'udp':
            socktype = socket.SOCK_DGRAM
        else:
            raise ValueError('%s unsupported' % inputinet['proto'])

        if inputinet['family'] == 'inet':
            sockfam = socket.AF_INET
        elif inputinet['family'] == 'inet6':
            sockfam = socket.AF_INET6
        else:
            raise ValueError('%s unsupported' % inputinet['family'])

        sock = socket.socket(sockfam,socktype)
        sock.bind((inputinet['ip'],inputinet['port']))
        pid = os.getpid()

        if addtimestamp:
            strftimefmt = timestampformat
        else:
            strftimefmt = ''

        # Set up a unique channel for our AMQP producer using a
        # pre-existing TCP connection to the relevant daemon.
        myhost = '%s:%i' % (amqphost,amqpport)
        if None == self.amqpchannels[myhost]:
            self.amqpchannels[myhost] = 1
        else:
            self.amqpchannels[myhost] += 1

        amqpchannel = self.amqpconnections[myhost].channel(
                channel_number=self.amqpchannels[myhost])
        amqpchannel.queue_declare(queue=name)

        # Wait indefinitely for data on our socket and send anything we
        # receive to AMQP
        while True:
            try:
                amqpchannel.basic_publish(
                        exchange='',
                        routing_key=name,
                        body=datetime.now().strftime(strftimefmt) + \
                                sock.recv(inputinet['bufsize']),
                        )
            except KeyboardInterrupt:
                amqpconnection.close()
        amqpconnection.close()

    def consumerlogger(self,log_filename,addtimestamp,timestampformat):
        '''Create a logging object that rotates itself every UTC midnight

        inetlogger = self.consumerlogger(os.path.join(directory,'%s.log' % name),
                addtimestamp,timestampformat)
        inetlogger.info(data)
        '''

        # Attempt to make directories
        if not os.path.exists(dirname):
            os.makedirs(os.path.dirname(log_filename))

        myprocname = multiprocessing.current_process().name

        # Set up a specific logger with our desired output level
        my_logger = logging.getLogger(myprocname)
        my_logger.setLevel(logging.DEBUG)

        # For basic logging, our producer should have done what we
        # need, so just do the default behavior of recoring %(message)s
        # FUTURE: convert specific data types here?
        formatter = logging.Formatter()

        handler = logging.handlers.TimedRotatingFileHandler(
                log_filename, when='midnight', utc=True)
        handler.setFormatter(formatter)
        my_logger.addHandler(handler)

        return my_logger

    def parseargs(self):
        parser = ArgumentParser()
        parser.add_argument('-d','--debug', help="increase output verbosity to DEBUG",
                                    action="store_true")
        parser.add_argument('-v','--verbose', help="increase output verbosity",
                                    action="store_true")
        parser.add_argument('-q','--quiet', help="decrease output verbosity",
                                    action="store_true")
        self.args = parser.parse_args()

        if self.args.debug:
            self.loglevel = logging.DEBUG
        elif self.args.verbose:
            self.loglevel = logging.INFO
        elif self.args.quiet:
            self.loglevel = logging.CRITICAL

    def parseandprocessyaml(self,yamlfile='pysomtsdatalogger.yaml'):
        '''Read our YAML file and configure ourself'''
        stream = file(yamlfile,'r')
        self.conf = yaml.load(stream)

        # Massage conf data
        for k,v in self.conf['producers'].items():

            # Merge whatever our channel settings are with the defaults
            mydict = self.default_producer_values.copy()
            mydict.update(v)

            # Populate a dict of AMQP hosts we'll connect to.
            self.amqphostports.append((mydict['amqphost'],mydict['amqpport']))

            # Process some specific keys
            for i in mydict.keys():
                if i == 'directory':
                    # If a dirname wasn't provided, use module name
                    if '' == mydict['directory']: mydict['directory'] = k

                    # mux dirname into absolute path
                    mydict['directory'] = os.path.join(
                            self.conf['main']['basedirectory'],
                            mydict['directory'])

                elif i == 'inputinet':
                    if 'inputinet' in v:
                        myd = self.default_inputinet_values.copy()
                        myd.update(mydict[i])
                        mydict[i] = myd
                        del myd
                    else:
                        del mydict[i]

                elif i == 'inputserial':
                    if 'inputserial' in v:
                        myd = self.default_inputserial_values.copy()
                        myd.update(mydict[i])
                        mydict[i] = myd
                        del myd
                    else:
                        del mydict[i]

            # Overwrite merged/processed dict for our channel
            self.conf['producers'][k] = mydict

        # We only want to establish one TCP connection to each
        # unique AMQP host, so we nake sure our list of hosts to
        # connect to is unique.
        self.amqphostports = set(self.amqphostports)

        self.logger.debug("processed YAML CONFIG:\n%s" % yaml.dump(self.conf))
        self.validate_conf()

    def validate_conf(self):
        '''Check various sections of our parsed conf against a schema'''

        for key,val in self.conf.items():
            # Validate main section, which is different than others.
            if key == 'main':
                self.schema_main(val)
            elif key == 'consumers':
                for k,v in val.items():
                    self.schema_consumer(v)
            elif key == 'producers':
                for k,v in val.items():
                    # Otherwise, validate channel sections
                    self.schema_producer(v)

                    # Our data is pretty structured, so some
                    # subsections of a channel need checking too.
                    for i in v.keys():
                        if i == 'inputserial':
                            # Validate serial dict
                            self.schema_producer_inputserial(v[i])

                        elif i == 'inputinet':
                            # Validate inet dict
                            self.schema_producer_inputinet(v[i])

                            # Validate non-special IP addresses
                            if 'ip' in (v[i]) and '' != v[i]['ip'] and \
                                    '<broadcast>' != v[i]['ip']:
                                socket.inet_aton(v[i]['ip'])
            else:
                raise ValueError('%s as a root key unsupported' % s)

def main():
    datalogger = PySOMTSDataLogger()

if __name__ == "__main__":
    main()
