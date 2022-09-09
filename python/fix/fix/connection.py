from datetime import datetime
import logging
from socket import SHUT_RDWR, SOL_TCP, TCP_NODELAY, socket
import sys
import time
from typing import Iterator, Optional, Union

import gevent
from gevent.event import Event
from gevent.lock import BoundedSemaphore
import simplefix
from simplefix import FixMessage, FixParser
from simplefix.errors import ParsingError
from simplefix.message import fix_val
from werkzeug.datastructures import ImmutableMultiDict

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


class FixConnection:
    def __init__(self, sock: socket, sender_id: str, target_id: Optional[str] = None) -> None:
        logger.debug(f"constructing new connection object [{target_id=}]")
        self._sock = sock
        sock.setsockopt(SOL_TCP, TCP_NODELAY, 1)
        self._next_send_seq_num = 1
        self._next_recv_seq_num = 1
        self._sender_id = sender_id
        self._target_id = target_id
        self._last_send_time = time.time()
        self._last_recv_time = time.time()
        self._heartbeat_interval = 30.
        self._has_session = False
        self._disconnected = Event()
        self._send_lock = BoundedSemaphore(1)

        gevent.spawn(self._close_on_exit)

        self.messages = self._get_messages()

    @property
    def connected(self) -> bool:
        return not self._disconnected.is_set()

    def _get_messages(self) -> Iterator[FixMessage]:
        try:
            for msg in self._read_messages():
                if not self._validate_message(msg):
                    continue

                if msg.message_type == simplefix.MSGTYPE_HEARTBEAT:
                    pass
                elif msg.message_type == simplefix.MSGTYPE_TEST_REQUEST:
                    self._send_heartbeat(msg.get(simplefix.TAG_TESTREQID))
                elif msg.message_type == simplefix.MSGTYPE_LOGOUT:
                    self.close()
                else:
                    yield msg
        except Exception:
            logger.exception("_get_messages errored")
        finally:
            self.close()

    def _read_messages(self) -> Iterator[FixMessage]:
        logger.debug("reading messages...")
        parser = FixParser()
        while True:
            gevent.idle()
            try:
                logger.debug("reading from socket...")
                buf = self._sock.recv(4096)
            except OSError:
                logger.debug("recv failed", exc_info=True)
                sys.exit(1)
                return
            if not buf:
                return
            logger.debug("received (%s)", buf)
            parser.append_buffer(buf)

            while True:
                try:
                    msg = parser.get_message()
                except ParsingError:
                    logger.warning('Error parsing FIX message', exc_info=True)
                    return
                if msg is None:
                    break
                yield msg

    def _validate_message(self, msg: FixMessage) -> bool:
        try:
            # Hack to make msg.get return decoded strings.
            decoded = ImmutableMultiDict([(k, v.decode()) for k, v in msg.pairs])
            msg.get = lambda key: decoded.get(fix_val(key))
        except ValueError:
            self.reject_message(
                msg, reason='Invalid encoding',
                error_code=simplefix.SESSIONREJECTREASON_INCORRECT_DATA_FORMAT_FOR_VALUE,
            )
            return False

        if self._target_id is None and msg.get(simplefix.TAG_SENDER_COMPID):
            self._target_id = msg.get(simplefix.TAG_SENDER_COMPID)

        if msg.get(simplefix.TAG_MSGSEQNUM):
            if msg.get(simplefix.TAG_MSGSEQNUM) == str(self._next_recv_seq_num):
                self._next_recv_seq_num += 1
            else:
                self.reject_message(
                    msg, reason='Incorrect sequence number',
                    tag_id=simplefix.TAG_MSGSEQNUM,
                    error_code=simplefix.SESSIONREJECTREASON_VALUE_INCORRECT_FOR_THIS_TAG)
                return False

        for tag, description in [(simplefix.TAG_MSGTYPE, 'message type'),
                                 (simplefix.TAG_BEGINSTRING, 'begin string'),
                                 (simplefix.TAG_SENDER_COMPID, 'sender ID'),
                                 (simplefix.TAG_TARGET_COMPID, 'target ID'),
                                 (simplefix.TAG_SENDING_TIME, 'sending time'),
                                 (simplefix.TAG_MSGSEQNUM, 'sequence number'),
                                 ]:
            if not msg.get(tag):
                self.reject_message(msg, reason=f'Missing {description}',
                                    tag_id=tag,
                                    error_code=simplefix.SESSIONREJECTREASON_REQUIRED_TAG_MISSING)
                return False

        if msg.get(simplefix.TAG_BEGINSTRING) != 'FIX.4.2':
            self.reject_message(
                msg, reason='Invalid FIX version',
                tag_id=simplefix.TAG_BEGINSTRING,
                error_code=simplefix.SESSIONREJECTREASON_VALUE_INCORRECT_FOR_THIS_TAG)
            return False
        elif msg.get(simplefix.TAG_SENDER_COMPID) != self._target_id:
            self.reject_message(
                msg, reason='Incorrect sender',
                tag_id=simplefix.TAG_SENDER_COMPID,
                error_code=simplefix.SESSIONREJECTREASON_VALUE_INCORRECT_FOR_THIS_TAG)
            return False
        elif msg.get(simplefix.TAG_TARGET_COMPID) != self._sender_id:
            self.reject_message(
                msg, reason='Incorrect target',
                tag_id=simplefix.TAG_TARGET_COMPID,
                error_code=simplefix.SESSIONREJECTREASON_VALUE_INCORRECT_FOR_THIS_TAG)
            return False

        self._last_recv_time = time.time()

        return True

    def send(self, values: dict) -> None:
        with self._send_lock:
            msg = FixMessage()
            msg.append_pair(simplefix.TAG_BEGINSTRING, 'FIX.4.2')
            msg.append_pair(simplefix.TAG_SENDER_COMPID, self._sender_id)
            msg.append_pair(simplefix.TAG_TARGET_COMPID, self._target_id)
            msg.append_pair(simplefix.TAG_MSGSEQNUM, self._next_send_seq_num)
            for key, value in values.items():
                if isinstance(value, datetime):
                    msg.append_utc_timestamp(key, value)
                else:
                    msg.append_pair(key, value)
            if not msg.get(simplefix.TAG_SENDING_TIME):
                msg.append_utc_timestamp(simplefix.TAG_SENDING_TIME)
            encoded = msg.encode()
            self._last_send_time = time.time()
            self._next_send_seq_num += 1

            try:
                logger.info('sending %s', encoded.replace(b'\x01', b'|'))
                self._sock.sendall(encoded)
            except OSError:
                logger.exception("an error occurred while sending")
                self.close(clean=False)
                return

            if msg.message_type == simplefix.MSGTYPE_LOGON:
                self._has_session = True

    def reject_message(self, msg: FixMessage, reason: str, *,
                       tag_id: Optional[Union[bytes, int]] = None,
                       error_code: Union[bytes, int]) -> None:
        params = {
            simplefix.TAG_MSGTYPE: simplefix.MSGTYPE_REJECT,
            simplefix.TAG_REFSEQNUM: msg.get(simplefix.TAG_MSGSEQNUM),
            372: msg.message_type,
            simplefix.TAG_TEXT: reason,
            simplefix.TAG_SESSIONREJECTREASON: error_code,
        }
        if tag_id is not None:
            params[371] = tag_id
        self.send(params)

    # Run this every few seconds
    def _maybe_send_heartbeat(self) -> None:
        if time.time() - self._last_send_time > self._heartbeat_interval:
            self._send_heartbeat()

    def _send_heartbeat(self, test_req_id: Optional[str] = None) -> None:
        if not self._has_session:
            return
        logger.debug("sending heartbeat...")
        data = {
            simplefix.TAG_MSGTYPE: simplefix.MSGTYPE_HEARTBEAT,
        }
        if test_req_id:
            data[simplefix.TAG_TESTREQID] = test_req_id
        self.send(data)

    # Run this every few seconds
    def _check_last_message_time(self) -> None:
        elapsed = time.time() - self._last_recv_time
        if elapsed > self._heartbeat_interval + 10:
            self.close()
        elif elapsed > self._heartbeat_interval and self._has_session:
            self.send({
                simplefix.TAG_MSGTYPE: simplefix.MSGTYPE_TEST_REQUEST,
                simplefix.TAG_TESTREQID: datetime.now(),
            })

    def close(self, clean: bool = True) -> None:
        if self._disconnected.is_set():
            return
        logger.debug("close was called, and we're closing. [clean=%r]", clean)
        self._disconnected.set()
        if clean and self._has_session:
            self._has_session = False
            self.send({
                simplefix.TAG_MSGTYPE: simplefix.MSGTYPE_LOGOUT,
            })
            try:
                self._sock.shutdown(SHUT_RDWR)
            except OSError:
                self._sock.close()
        else:
            self._sock.close()

    def _close_on_exit(self) -> None:
        gevent.wait([self._disconnected], count=1)
        logger.debug("closing on exit")
        if self.connected:
            self.close()
