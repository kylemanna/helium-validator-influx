#!/usr/bin/env python3

from datetime import datetime, timezone
from dataclasses import dataclass
from collections import namedtuple
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import PointSettings
from miner_client import MinerClient

from functools import partial

import json
import logging
import subprocess
import time
import sched

log_name = 'validator-monitor' if __name__ == '__main__' else '.'.join((__name__.split('.')[-2:]))
log = logging.getLogger(log_name)

SchedCmd    = namedtuple('SchedCmd', 'method, handler, sched, validator')
SchedParams = namedtuple('SchedParams', 'period, delay, priority')
InfluxCtx   = namedtuple('InfluxCtx', 'client, bucket, write_api')
Validator   = namedtuple('Validator', 'mc, name, addr, summary')

def str_to_native(s):
    """Convert a string to a native pythonic type if possible"""
    if type(s) != str:
        return s

    # Attempt to convert string ['0', '1', '23', '0x20'] -> int()
    # Fails on ['1.1', 'True']
    try:
        return int(s, 0)
    except:
        pass

    # Attempt to convert string float
    try:
        return float(s)
    except:
        pass

    return s


def info_p2p_status_handler(pt, raw_result):
    """ Example:
    >>> mc.info_p2p_stats()
    {'connected': 'yes', 'dialable': 'yes', 'height': 1007194, 'nat_type': 'none'}
    """

    p2p_status = raw_result
    pt.field('height',   p2p_status['height'])
    pt.field('nat_type', p2p_status['nat_type'])
    pt.field('connected', 1 if p2p_status['connected'] == 'yes'  else 0)
    pt.field('dialable',  1 if p2p_status['dialable']  == 'yes'  else 0)
    pt.field('nat',       0 if p2p_status['nat_type']  == 'none' else 1)

    return pt

def peer_book_handler(pt, raw_result):
    """ Example:
    >>> print(json.dumps(mc.peer_book('self')[0], indent=2))
    {
      "address": "/p2p/13zjYv9VDPubyLKxX7asVGzTbtzp3hBhqRXRroaTLjJbuPHXquC",
      "connection_count": 15,
      "last_updated": "253.873",
      "listen_addr_count": 1,
      "listen_addresses": [
        "/ip4/70.140.215.106/tcp/2154"
      ],
      "name": "tricky-teal-tiger",
      "nat": "none",
      "sessions": [
        {
          "local": "/ip4/172.20.0.2/tcp/2154",
          "name": "immense-ginger-frog",
          "p2p": "/p2p/1124HEFuFcdtvSeZeD1QfnrBgLTvxbDJzdzQncshaEtcnuep3RRh",
          "remote": "/ip4/67.180.217.101/tcp/44158"
        },
      ]
    }
    """

    peer_book = raw_result[0]
    #print(f"peer_book: {json.dumps(peer_book)}")

    pt.field('connection_count', peer_book['connection_count'])
    pt.field('listen_addr_count', peer_book['listen_addr_count'])
    pt.field('nat', 0 if peer_book['nat'] == 'none' else 1)
    pt.field('session_count', len(peer_book['sessions']))

    try:    pt.field('last_updated', int(peer_book['last_updated']))
    except: pass

    return pt

def ledger_validators_handler(pt, raw_result):
    """ Example:
    >>> print(json.dumps(mc.ledger_validators()[0], indent=4))
    {
        "address": "1ZRHW1VCMgazNhKpGzAmbNN9RBf2gDKZGW4LvkigGtwBUpandW2",
        "dkg_penalty": 0.0,
        "last_heartbeat": 1007179,
        "name": "sticky-purple-frog",
        "nonce": 1,
        "owner_address": "12yYJYcqjc3kAhiZnB9Q1jhQs5hoL1VJATh2w6LGejPtF4X1zUw",
        "performance_penalty": 0.0,
        "stake": 0,
        "status": "unstaked",
        "tenure_penalty": 0.0,
        "total_penalty": 0.0,
        "version": 1
    }
    """

    # Unstaked/invalid validators will return a NoneType
    if not raw_result:
        return

    try:
        validator = raw_result
        keys = ['dkg_penalty', 'last_heartbeat', 'performance_penalty', 'stake', 'status', 'tenure_penalty', 'total_penalty', 'version']
        for k in keys:
            pt.field(k, validator[k])
        pt.tag('addr', validator['address'])
        pt.tag('name', validator['name'])
        return pt
    except Exception as e:
        log.debug(f"Error in ledger_validators_handler: {e}")
        pass

def ledger_balance_handler(pt, raw_result):
    """ Example:
    >>> print(json.dumps(mc.ledger_balance(a), indent=4))
    {
        "address": "13M48crbuT1XqsAti5wrHcPLzdAGKxtF8zEgPyNJESCu3drvLyB",
        "balance": 1416024606,
        "nonce": 0
    }

    """

    try:
        balance = raw_result
        # Override addr since this has nothing to do with validator address
        pt.tag('addr', balance['address'])
        pt.field('balance', balance['balance'])
        pt.field('nonce', balance['nonce'])
        return pt
    except:
        pass

class CmdScheduler(sched.scheduler):
    @dataclass
    class CmdContext:
        """Tracking context related to a command"""
        cmd: SchedCmd
        next: float

    def __init__(self, influx, cmds):
        super().__init__()

        self._influx = influx

        start = time.monotonic()
        for cmd in cmds:
            ctx = self.CmdContext(cmd, start + cmd.sched.delay)
            self.enterabs(ctx.next, cmd.sched.priority, self.run_cmd, (ctx,))

    def run_cmd(self, ctx):
        # Reschedule
        cmd = ctx.cmd
        while ctx.next < time.monotonic():
            ctx.next += cmd.sched.period
            self.enterabs(ctx.next, cmd.sched.priority, self.run_cmd, (ctx,))

        raw_result = cmd.method()
        #log.debug(f"{cmd.method} -> {raw_result}")

        measurement_name = cmd.method.func.__name__ if isinstance(cmd.method, partial) else cmd.method.__name__

        # Can't use PointSettings when instantiating the WriteApi as it'll
        # override the `addr` tag when we don't want it to.
        pt = Point(measurement_name)
        pt.time(datetime.now(timezone.utc))
        pt.tag('addr', cmd.validator.addr)
        pt.tag('name', cmd.validator.name)

        result = ctx.cmd.handler(pt, raw_result)
        #log.debug(f"Point(s) {result}")

        if result:
            self._influx.write_api.write(self._influx.bucket, None, result)


if __name__ == '__main__':
    # Don't set globally incase used as module which should set this.
    logging.basicConfig(level=logging.INFO)
    #format='%(asctime)s %(levelname)-8s %(message)s',
    #datefmt='%Y-%m-%d %H:%M:%S'

    # Info level prints every request, quiet it down.
    logging.getLogger('jsonrpcclient.client').setLevel(logging.WARNING)
    logging.getLogger('charset_normalizer').setLevel(logging.WARNING)

    # Load configuration
    import configparser
    cfg = configparser.ConfigParser()
    cfg.read('config.ini')

    client = InfluxDBClient.from_config_file("config.ini")

    validators = list()
    for h in cfg['validator']['host'].split(','):
        mc = MinerClient(host=h)
        addr = mc.peer_addr().removeprefix('/p2p/')
        summary = mc.info_summary()
        validators += [ Validator(mc, h, addr, summary) ]

    # FIXME with mc.info_version() when it's merged :-/, this is very slow and expensive
    # FIXME poll the version periodically

    tags = { 'net': 'main' }
    ps = PointSettings(**{k: str(v) for k, v in tags.items()})
    write_api = client.write_api(point_settings=ps)

    influx = InfluxCtx(client, cfg['general']['bucket'], write_api)

    """
    Things to monitor ref: https://docs.helium.com/mine-hnt/validators/monitoring/
    * miner info p2p_status
    * miner peer book -s
    * miner ledger validators (self)
    * miner hbbft perf (self) - hard to use, crashes if not foudn
    """

    # Can be self or a real address to save on json rpc invocation

    cmds = []
    for v in validators:
        cmds += [ SchedCmd(v.mc.info_p2p_status,            info_p2p_status_handler,   SchedParams(60, 0, 10), v) ]
        cmds += [ SchedCmd(partial(v.mc.peer_book, 'self'), peer_book_handler,         SchedParams(60, 0, 10), v) ]
        #SchedCmd(mc.hbbft_perf,        miner_parser_handler, SchedParams(15, 0, 10)),

    query_validator = validators[0]
    if 'ledger' in cfg:
        if 'addresses' in cfg['ledger']:
            for a in cfg['ledger']['addresses'].split(','):
                log.info(f"Monitoring address {a}")
                cmds += [SchedCmd(partial(mc.ledger_balance, a), ledger_balance_handler, SchedParams(60, 0, 10), query_validator)]

        if 'validators' in cfg['ledger']:
            for v in cfg['ledger']['validators'].split(','):
                log.info(f"Monitoring validator {v}")
                cmds += [SchedCmd(partial(mc.ledger_validators, v), ledger_validators_handler, SchedParams(60, 0, 10), query_validator)]

    # Run forever
    cs = CmdScheduler(influx, cmds)
    cs.run()
