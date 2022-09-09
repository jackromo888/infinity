from contextlib import ExitStack, closing
from datetime import datetime
from decimal import Decimal
import hmac
import logging
from socket import create_connection
import ssl
import sys
from typing import Optional
from urllib.parse import urlparse
from uuid import uuid4

import gevent
from gevent.event import Event
import simplefix
from simplefix.message import fix_val

from fix.connection import FixConnection

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


class FixClient:
    """FIX client to use for testing."""

    def __init__(self, *, url: str, client_id: str, target_id: str,
                 subaccount_name: Optional[str] = None) -> None:
        self._url = url
        self._client_id = client_id
        self._target_id = target_id
        self._conn: Optional[FixConnection] = None
        self._connected = Event()
        self._next_seq_num = 1
        self._subaccount_name = subaccount_name

    def connect(self) -> None:
        if self._connected.is_set():
            return
        gevent.spawn(self.run)
        gevent.wait([self._connected])
        logger.debug("connected!")

    def run(self) -> None:
        parsed_url = urlparse(self._url)
        assert parsed_url.port is not None

        try:
            with ExitStack() as stack:
                logger.debug(
                    "creating connection [hostname=%s, port=%d]",
                    parsed_url.hostname,
                    parsed_url.port)
                sock = stack.enter_context(create_connection((parsed_url.hostname,
                                                              parsed_url.port)))

                logger.debug("connected to socket %r", sock)
                assert 'ssl' in parsed_url.scheme or 'tls' in parsed_url.scheme
                logger.debug("creating SSL context")
                context = ssl.create_default_context()
                logger.debug("wrapping socket with SSL context")
                sock = stack.enter_context(context.wrap_socket(sock,
                                                               server_hostname=parsed_url.hostname))
                logger.debug("socket version = %s", sock.version())

                logger.debug("creating FixConnection")
                conn: FixConnection = stack.enter_context(
                    closing(FixConnection(sock, self._client_id, self._target_id)))
                self._conn = conn
                self._connected.set()

                for msg in conn.messages:
                    logger.info('received message (%s)', msg)
                logger.debug('disconnected.')
                sys.exit(1)
        except Exception:
            logger.exception("does this happen here? Yow")

    def send(self, values: dict) -> None:
        self.connect()
        assert self._connected.is_set()
        assert self._conn is not None
        self._conn.send(values)

    def login(self, secret: str, cancel_on_disconnect: Optional[str] = None) -> None:
        send_time_str = datetime.utcnow().strftime('%Y%m%d-%H:%M:%S')
        sign_target = b'\x01'.join([fix_val(val) for val in [
            send_time_str,
            simplefix.MSGTYPE_LOGON,
            self._next_seq_num,
            self._client_id,
            self._target_id,
        ]])
        signed = hmac.new(secret.encode(), sign_target, 'sha256').hexdigest()
        login_payload = {
            simplefix.TAG_MSGTYPE: simplefix.MSGTYPE_LOGON,
            simplefix.TAG_SENDING_TIME: send_time_str,
            simplefix.TAG_ENCRYPTMETHOD: 0,
            simplefix.TAG_HEARTBTINT: 30,
            simplefix.TAG_RAWDATA: signed,
            **({8013: cancel_on_disconnect} if cancel_on_disconnect else {}),
            **({simplefix.TAG_ACCOUNT: self._subaccount_name} if self._subaccount_name else {}),
        }
        logger.info("logging in with %r", login_payload)
        self.send(login_payload)

    def send_heartbeat(self, test_req_id: Optional[str] = None) -> None:
        req = {
            simplefix.TAG_MSGTYPE: simplefix.MSGTYPE_HEARTBEAT,
        }
        if test_req_id:
            req[simplefix.TAG_TESTREQID] = test_req_id
        self.send(req)

    def send_test_request(self, test_req_id: str) -> None:
        self.send({
            simplefix.TAG_MSGTYPE: simplefix.MSGTYPE_TEST_REQUEST,
            simplefix.TAG_TESTREQID: test_req_id,
        })

    def request_order_status(self, order_id: str) -> None:
        self.send({
            simplefix.TAG_MSGTYPE: simplefix.MSGTYPE_ORDER_STATUS_REQUEST,
            simplefix.TAG_ORDERID: order_id,
        })

    def cancel_all_limit_orders(self, market: Optional[str] = None,
                                client_cancel_id: Optional[str] = None) -> None:
        self.send({
            simplefix.TAG_MSGTYPE: simplefix.MSGTYPE_ORDER_MASS_CANCEL_REQUEST,
            530: 1 if market else 7,
            **({simplefix.TAG_CLORDID: client_cancel_id} if client_cancel_id else {}),
            **({simplefix.TAG_SYMBOL: market} if market else {}),
        })

    def send_order(self, symbol: str, side: str, price: Decimal, size: Decimal,
                   reduce_only: bool = False, client_order_id: Optional[str] = None,
                   ioc: bool = False) -> None:
        self.send({
            simplefix.TAG_MSGTYPE: simplefix.MSGTYPE_NEW_ORDER_SINGLE,
            simplefix.TAG_HANDLINST: simplefix.HANDLINST_AUTO_PRIVATE,
            simplefix.TAG_CLORDID: uuid4() if client_order_id is None else client_order_id,
            simplefix.TAG_SYMBOL: symbol,
            simplefix.TAG_SIDE: (simplefix.SIDE_BUY if side == 'buy'
                                 else simplefix.SIDE_SELL),
            simplefix.TAG_PRICE: price,
            simplefix.TAG_ORDERQTY: size,
            simplefix.TAG_ORDTYPE: simplefix.ORDTYPE_LIMIT,
            simplefix.TAG_TIMEINFORCE: simplefix.TIMEINFORCE_GOOD_TILL_CANCEL if not ioc else
            simplefix.TIMEINFORCE_IMMEDIATE_OR_CANCEL,
            **({simplefix.TAG_EXECINST: simplefix.EXECINST_DO_NOT_INCREASE} if reduce_only else {}),
        })

    def cancel_order(self, order_id: Optional[str] = None,
                     client_order_id: Optional[str] = None) -> None:
        req = {
            simplefix.TAG_MSGTYPE: simplefix.MSGTYPE_ORDER_CANCEL_REQUEST,
        }
        if order_id is not None:
            req[simplefix.TAG_ORDERID] = order_id
        if client_order_id is not None:
            req[simplefix.TAG_CLORDID] = client_order_id
        self.send(req)
