"""rpc/client.py: both transports return the same values and share
run_cli's retry/error semantics. The HTTP side runs against a tiny fake
bitcoind; the CLI side against a fake bitcoin-cli script."""

from __future__ import annotations

import json
import socketserver
import sys
import tempfile
import textwrap
import threading
import time
import unittest
from decimal import Decimal
from http.server import BaseHTTPRequestHandler
from pathlib import Path

from btc_parser_app.rpc import client
from btc_parser_app.rpc.client import RpcCliError, RpcError, cookie_path, http_endpoint
from test_catchup import make_config

BLOCK_JSON = '{"hash":"00ab","height":7,"difficulty":1.5,"tx":[{"fee":0.00012345,"vout":[{"value":21000000.00000001}]}]}'


class FakeServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    # Not http.server.HTTPServer: its server_bind() does a reverse-DNS
    # getfqdn() that can stall for 30s+ on some hosts.
    daemon_threads = True


class FakeBitcoind(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"  # keep-alive, like bitcoind
    expected_auth = ""
    calls: list[str] = []

    def log_message(self, *args) -> None:
        pass

    def do_POST(self) -> None:
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        type(self).calls.append(body["method"])
        if self.headers.get("Authorization") != self.expected_auth:
            self.send_response(401)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        method, params = body["method"], body["params"]
        status, error, result = 200, "null", "null"
        if method == "getblockhash":
            result = json.dumps(f"hash-{params[0]}")
        elif method == "getblock":
            result = BLOCK_JSON
        elif method == "gettxout":
            result = "null"
        elif method == "getrawtransaction":
            status, error = 500, '{"code":-5,"message":"No such mempool transaction"}'
        else:
            status, error = 404, '{"code":-32601,"message":"Method not found"}'
        payload = f'{{"result":{result},"error":{error},"id":{body["id"]}}}'.encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


def basic(userpass: str) -> str:
    import base64

    return "Basic " + base64.b64encode(userpass.encode()).decode()


class EndpointDerivationTest(unittest.TestCase):
    def test_defaults_follow_bitcoin_cli(self) -> None:
        cfg = make_config()
        self.assertEqual(http_endpoint(cfg), ("127.0.0.1", 8332))
        self.assertEqual(cookie_path(cfg), Path.home() / ".bitcoin" / ".cookie")

    def test_from_extra_args(self) -> None:
        cfg = make_config(extra_args=("-rpcconnect=10.42.5.56", "-rpcport=8332", "-rpccookiefile=/bitcoin/.cookie"))
        self.assertEqual(http_endpoint(cfg), ("10.42.5.56", 8332))
        self.assertEqual(cookie_path(cfg), Path("/bitcoin/.cookie"))

    def test_datadir_and_chain(self) -> None:
        cfg = make_config(extra_args=("-datadir=/srv/btc", "-regtest"))
        self.assertEqual(http_endpoint(cfg), ("127.0.0.1", 18443))
        self.assertEqual(cookie_path(cfg), Path("/srv/btc/regtest/.cookie"))
        cfg = make_config(extra_args=("-rpcconnect=node:9999",))
        self.assertEqual(http_endpoint(cfg), ("node", 9999))

    def test_explicit_settings_win(self) -> None:
        cfg = make_config(
            extra_args=("-rpcconnect=10.0.0.1", "-rpccookiefile=/x/.cookie"),
            http_url="http://127.0.0.1:18555",
            cookie_file=Path("/var/lib/bitcoin/.cookie"),
        )
        self.assertEqual(http_endpoint(cfg), ("127.0.0.1", 18555))
        self.assertEqual(cookie_path(cfg), Path("/var/lib/bitcoin/.cookie"))


class HttpTransportTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.cookie = Path(self._tmp.name) / ".cookie"
        self.cookie.write_text("__cookie__:one")
        FakeBitcoind.expected_auth = basic("__cookie__:one")
        FakeBitcoind.calls = []
        self.server = FakeServer(("127.0.0.1", 0), FakeBitcoind)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.cfg = make_config(
            transport="http",
            http_url=f"http://127.0.0.1:{self.server.server_address[1]}",
            cookie_file=self.cookie,
            max_cli_retries=2,
            cli_retry_backoff_seconds=0,
        )

    def tearDown(self) -> None:
        if self.server is not None:
            self.server.shutdown()
            self.server.server_close()
        self._tmp.cleanup()

    def test_values_match_the_cli_parse(self) -> None:
        self.assertEqual(client.get_block_hash(self.cfg, 7), "hash-7")
        block = client.get_block(self.cfg, "00ab")
        self.assertEqual(block, json.loads(BLOCK_JSON, parse_float=Decimal))
        self.assertEqual(block["tx"][0]["vout"][0]["value"], Decimal("21000000.00000001"))
        self.assertIsNone(client.get_tx_out(self.cfg, "aa", 0, True))
        self.assertEqual(FakeBitcoind.calls.count("getblock"), 1)

    def test_rotated_cookie_is_reread(self) -> None:
        client.get_block_hash(self.cfg, 1)
        self.cookie.write_text("__cookie__:two")
        FakeBitcoind.expected_auth = basic("__cookie__:two")
        self.assertEqual(client.get_block_hash(self.cfg, 2), "hash-2")
        # 401 on the kept-alive connection, then a reconnect with the new cookie.
        self.assertEqual(FakeBitcoind.calls, ["getblockhash"] * 3)

    def test_server_closed_idle_connection_reconnects_without_backoff(self) -> None:
        cfg = make_config(**{**self.cfg.__dict__, "cli_retry_backoff_seconds": 30})
        client.get_block_hash(cfg, 1)
        # What bitcoind does after -rpcservertimeout: close the idle socket.
        conn = client._http_connection(cfg)._conn
        assert conn is not None and conn.sock is not None
        import socket

        conn.sock.shutdown(socket.SHUT_RDWR)
        started = time.monotonic()
        self.assertEqual(client.get_block_hash(cfg, 2), "hash-2")
        self.assertLess(time.monotonic() - started, 5)

    def test_rpc_error_without_retry(self) -> None:
        with self.assertRaises(RpcError):
            client.get_raw_transaction(self.cfg, "aa")
        self.assertEqual(FakeBitcoind.calls, ["getrawtransaction"])

    def test_rpc_error_with_retry_then_gives_up(self) -> None:
        with self.assertRaises(RpcCliError) as ctx:
            client.get_chain_tips(self.cfg)  # fake answers "Method not found"
        self.assertNotIsInstance(ctx.exception, RpcError)
        self.assertEqual(FakeBitcoind.calls, ["getchaintips"] * 3)

    def test_unreachable_node(self) -> None:
        port = self.server.server_address[1]
        self.server.shutdown()
        self.server.server_close()
        self.server = None
        cfg = make_config(
            transport="http",
            http_url=f"http://127.0.0.1:{port}",
            cookie_file=self.cookie,
            max_cli_retries=1,
            cli_retry_backoff_seconds=0,
        )
        with self.assertRaises(RpcCliError):
            client.get_block_count(cfg)


class CliTransportTest(unittest.TestCase):
    """The pre-existing bitcoin-cli path: same argv as before, same parsing."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        tmp = Path(self._tmp.name)
        self.argv_log = tmp / "argv.log"
        script = tmp / "bitcoin-cli"
        script.write_text(
            textwrap.dedent(
                f"""\
                #!{sys.executable}
                import sys
                args = sys.argv[1:]
                open({str(self.argv_log)!r}, "a").write(" ".join(args) + "\\n")
                method = args[0]
                if method == "getblockhash":
                    print("00" + args[1].zfill(62))
                elif method == "getblock":
                    print({BLOCK_JSON!r})
                elif method == "getblockcount":
                    print("845")
                elif method == "gettxout":
                    pass  # JSON null -> bitcoin-cli prints nothing
                elif method == "getrawtransaction":
                    print("error code: -5", file=sys.stderr)
                    sys.exit(5)
                """
            )
        )
        script.chmod(0o755)
        self.cfg = make_config(bitcoin_cli_path=str(script), max_cli_retries=1, cli_retry_backoff_seconds=0)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_values_and_argv(self) -> None:
        self.assertEqual(client.get_block_count(self.cfg), 845)
        self.assertEqual(client.get_block_hash(self.cfg, 12), "00" + "12".zfill(62))
        self.assertEqual(client.get_block(self.cfg, "00ab"), json.loads(BLOCK_JSON, parse_float=Decimal))
        self.assertIsNone(client.get_tx_out(self.cfg, "aa", 3, False))
        with self.assertRaises(RpcError):
            client.get_raw_transaction(self.cfg, "bb", "cc")
        self.assertEqual(
            self.argv_log.read_text().splitlines(),
            [
                "getblockcount",
                "getblockhash 12",
                "getblock 00ab 3",
                "gettxout aa 3 false",
                "getrawtransaction bb 2 cc",
            ],
        )


if __name__ == "__main__":
    unittest.main()
