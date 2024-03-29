# Source: https://github.com/andrewboudreau/miner_httpclient
import logging

from jsonrpcclient.clients.http_client import HTTPClient
from jsonrpcclient.exceptions import ReceivedErrorResponseError
from jsonrpcclient.requests import Request

logger = logging.getLogger(__name__)

# miner http api reference code
## https://github.com/helium/miner/tree/mra/jsonrpc/src/jsonrpc

class MinerClient:
    def __init__(self, scheme="http", host="localhost", port=4467, logging=False):
        self.url = f'{scheme}://{host}:{port}/jsonrpc'
        self.client = HTTPClient(self.url, basic_logging=logging)
        self.timeout = 30

    def http_post(self, method, **kwargs):
        try:
          if not kwargs:              
            response = self.client.send(Request(method), timeout=self.timeout)
          else:
            response = self.client.send(Request(method, **kwargs), timeout=self.timeout)

          return response.data.result

        except ReceivedErrorResponseError as ex:
            logging.error("id: %s method: '%s' message: %s", ex.response.id, method, ex.response.message)

    # account
    # https://github.com/helium/miner/tree/master/src/jsonrpc/miner_jsonrpc_accounts.erl
    def account_get(self, address):
        return self.http_post("account_get", address=address)
    
    # blocks
    # https://github.com/helium/miner/tree/master/src/jsonrpc/miner_jsonrpc_blocks.erl
    def block_height(self):
        return self.http_post("block_height")["height"]
    
    def block_get(self, height=None, hash=None):
        if height is None and hash is None:
          return self.http_post("block_get") 
        elif hash is None:
          return self.http_post("block_get", height=height)
        else:
          return self.http_post("block_get", hash=hash)

    # info
    # # https://github.com/helium/miner/tree/master/src/jsonrpc/miner_jsonrpc_info.erl
    def info_height(self):
        return self.http_post("info_height")
    
    def info_in_consensus(self):
        return self.http_post("info_in_consensus")["in_consensus"]
    
    def info_name(self):
        return self.http_post("info_name")["name"]
    
    def info_block_age(self):
        return self.http_post("info_block_age")["block_age"]
    
    def info_p2p_status(self):
        return self.http_post("info_p2p_status")
    
    def info_region(self):
        return self.http_post("info_region")
    
    def info_summary(self):
        return self.http_post("info_summary")

    # will work once #877 is merged, https://github.com/helium/miner/pull/887
    def info_version(self):
        return self.http_post("info_version")["version"]
    
    # dkg
    # https://github.com/helium/miner/tree/master/src/jsonrpc/miner_jsonrpc_dkg.erl
    
    # follow https://github.com/helium/miner/pull/909
    def dkg_status(self):
        return self.http_post("dkg_status")
        
    def dkg_queue(self):
        return self.http_post("dkg_queue")
    
    def dkg_next(self):
        return self.http_post("dkg_next")
    
    def dkg_running(self):
        return self.http_post("dkg_running")


    # hbbft
    # https://github.com/helium/miner/tree/master/src/jsonrpc/miner_jsonrpc_hbbft.erl
    def hbbft_status(self):
        return self.http_post("hbbft_status")

    def hbbft_queue(self):
        return self.http_post("hbbft_queue")

    def hbbft_skip(self):
        return self.http_post("hbbft_skip")["result"]

    def hbbft_perf(self, name=None):
        if name is None:
            return self.http_post("hbbft_perf")
        if name == "self":
            return self.by_name(self.http_post("hbbft_perf"), self.info_name())
        else:
            return self.by_name(self.http_post("hbbft_perf"), name)

    # ledger
    # https://github.com/helium/miner/tree/master/src/jsonrpc/miner_jsonrpc_ledger.erl
    def ledger_balance(self, address=None, htlc=None):
        if address is None and htlc is None:
          return self.http_post("ledger_balance") 
        elif htlc is None:
          return self.http_post("ledger_balance", address=address) 
        else:
          return self.http_post("ledger_balance", htlc=True)

    def ledger_gateways(self, verbose=False):
        return self.http_post("ledger_gateways", verbose=verbose)

    def ledger_validators(self, address=None):
        if address is None:
            return self.http_post("ledger_validators")
        if address == "self":
            return self.by_address(self.http_post("ledger_validators"), self.remove_prefix(self.peer_addr(), "/p2p/"))
        else:
            return self.by_address(self.http_post("ledger_validators"), self.remove_prefix(address, "/p2p/"))

    def ledger_variables(self, name=None):
        if name is None:
            return self.http_post("ledger_variables")
        else:
          return self.http_post("ledger_variables", name=name)[name]

    # peer
    # https://github.com/helium/miner/tree/master/src/jsonrpc/miner_jsonrpc_peer.erl
    def peer_session(self):
        return self.http_post("peer_session")["sessions"]

    def peer_listen(self):
        return self.http_post("peer_listen")

    def peer_addr(self):
        return self.http_post("peer_addr")["peer_addr"]

    def peer_connect(self, addr):
        return self.http_post("peer_connect", addr=addr)

    def peer_ping(self, addr):
        return self.http_post("peer_ping", addr=addr)

    def peer_book(self, addr):
        return self.http_post("peer_book", addr=addr)

    def peer_gossip_peers(self):
        return self.http_post("peer_gossip_peers")

    def peer_refresh(self, addr):
        return self.http_post("peer_refresh", addr=addr)

   # state channel
   # https://github.com/helium/miner/tree/master/src/jsonrpc/miner_jsonrpc_sc.erl
    def sc_active(self):
        return self.http_post("sc_active")

    def sc_list(self):
        return self.http_post("sc_list")

    # snapshot
    # https://github.com/helium/miner/tree/master/src/jsonrpc/miner_jsonrpc_snapshot.erl
    def snapshot_list(self):
        return self.http_post("snapshot_list")

    # txn
    # https://github.com/helium/miner/tree/master/src/jsonrpc/miner_jsonrpc_txn.erl
    def txn_queue(self):
        return self.http_post("txn_queue")

    def txn_add_gateway(self, owner):
        return self.http_post("txn_add_gateway", owner)

    def txn_assert_location(self, owner, location):
        return self.http_post("txn_assert_location", owner=owner, location=location)

    # txns
    # https://github.com/helium/miner/tree/master/src/jsonrpc/miner_jsonrpc_txns.erl
    def transaction_get(self, hash):
        return self.http_post("transaction_get", hash=hash)

    def by_name(self, validators, name):
        return next((v for v in validators if v["name"] == name), None)

    def by_address(self, validators, address):
        return next((v for v in validators if v["address"] == address), None)
    
    def remove_prefix(self, text, prefix):
        return text[text.startswith(prefix) and len(prefix):]
