#!/usr/bin/env python3

from datetime import datetime, timezone
from dataclasses import dataclass
from collections import namedtuple
from influxdb_client import InfluxDBClient, Point


import json
import logging
import subprocess
import time
import sched

log_name = 'validator-monitor' if __name__ == '__main__' else '.'.join((__name__.split('.')[-2:]))
log = logging.getLogger(log_name)

SchedCmd    = namedtuple('SchedCmd', 'args, parser, sched')
SchedParams = namedtuple('SchedParams', 'period, delay, priority')
InfluxCtx   = namedtuple('InfluxCtx', 'client, org, bucket, write_api')

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

class MinerParser():
    """Parse Helium Miner command responses"""
    headings = None

    def parse(self, line: str):
        """Parse a single miner command line response"""

        # Table formatting
        if line.startswith('+-') or line == '\n':
            return None

        d = tuple(filter(lambda x: len(x), (i.strip() for i in line.split('|'))))
        if d[0] == 'name':
            # heading row
            if not self.headings:
                self.headings = d
            return None

        try:
            if len(d) != len(self.headings):
                #log.error()
                print("Warning: heading and column lengths don't match, skipping")
                return None
        except:
            print(f"Error: {d=}")
            print(f"Error: {self.headings=}")
            # This may not occur if return code is properly checked? Raise to blow up.
            raise

        d = { k: str_to_native(v) for k, v in zip(self.headings, d)}
        updates = {}

        for k, v in d.items():
            if not isinstance(v, str): continue

            # Parse helium ratios and add a '${key}_percent' field to simplify downstream processing
            tmp = v.split('/')
            if len(tmp) == 2:
                tmp = tuple(int(i) for i in tmp)
                # InfluxDB doesn't udnerstand a list/tuple, overwrite the string
                #updates[k] = tmp
                updates[f"{k}"] = tmp[0]
                updates[f"{k}_total"] = tmp[1]
                # Handle divide by zero, don't report anything or it makes the percent graphs display wrong (instead we have no data)
                if tmp[1]:
                    updates[f"{k}_percent"] = float(tmp[0]/tmp[1])

        d.update(updates)

        return d

    def iter(self, resp: str):
        """Return a generator processing a command response"""
        row = None
        for line in resp.rstrip().splitlines():
            row = self.parse(line)
            if row:
                yield row


def miner_parser_handler(resp):
    """Parse Helium Miner output and frame appropriately"""

    # Correct truncated field names
    # TODO: Determine how to not get truncated field names in the first place without a pty
    # truncated cmds: "miner ledger validators"
    lut = { 'failur': 'failure',
            'versi': 'version',
            'last_heart': 'last_heartbeat'
          }

    return { row['name']: { lut.get(k, k): v for k, v in row.items() } for row in MinerParser().iter(resp) }

# TODO delete in favor of influxdb2 Points API
def make_measurement(timestamp, name, fields, measurement_name):
    d = { 'measurement': measurement_name,
          'tags': { 'entity_id': name, 'version': fields.get('version') },
          'time': str(timestamp),
          'fields': fields }
    return d


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

        try:
            measurement_time = datetime.now(timezone.utc)

            # TODO handle stderr
            cp = subprocess.run(cmd.args, universal_newlines=True, stdout=subprocess.PIPE)
            cp.check_returncode()
            raw_data = cmd.parser(cp.stdout)

            data = []
            measurement_name = '_'.join(cmd.args[cmd.args.index('miner')+1:])
            for validator_name, fields in raw_data.items():
                if validator_name.startswith('@'): continue
                data += [make_measurement(measurement_time, validator_name, fields, measurement_name)]

            log.debug(json.dumps(data))

            # TODO convert to Points API
            #p = Point(measurement_name)
            self._influx.write_api.write(self._influx.bucket, self._influx.org, data)

        except subprocess.CalledProcessError as cpe:
            log.warning(f"Exception: {cpe}")

if __name__ == '__main__':
    # Don't set globally incase used as module which should set this.
    logging.basicConfig(level=logging.INFO)
    #format='%(asctime)s %(levelname)-8s %(message)s',
    #datefmt='%Y-%m-%d %H:%M:%S'

    # Load configuration
    import configparser
    config_all = configparser.ConfigParser()
    config_all.read('config.ini')

    client = InfluxDBClient.from_config_file("config.ini")
    write_api = client.write_api()
    influx = InfluxCtx(client, config_all['influx2']['org'], config_all['general']['bucket'], write_api)

    # TODO Simpler invocation later?
    # /opt/miner/bin/nodetool -B -- -root /usr/local/lib/erlang -progname erl -- -home /root -- -boot no_dot_erlang -noshell -run escript start -- -name maintaf0915d1-val_miner@127.0.0.1 -setcookie miner -- -extra /opt/miner/bin/nodetool -name val_miner@127.0.0.1 rpc miner_console command hbbft perf

    base_cmd = ('docker', 'exec', 'crypto_helium-validator_1', 'nice', '-n20')
    cmds = (
        SchedCmd(base_cmd + ('miner', 'hbbft',  'perf'),       miner_parser_handler, SchedParams(15, 0, 10)),
        SchedCmd(base_cmd + ('miner', 'ledger', 'validators'), miner_parser_handler, SchedParams(20, 2, 100)),
        #SchedCmd(base_cmd + ('miner', 'info',   'p2p_status'), WRITE_ME, SchedParams(60, 4, 200))
        # TODO `ledger variables --all` + parser
        # TODO `peer book -s` + parser
    )

    # Run forever
    cs = CmdScheduler(influx, cmds)
    cs.run()
