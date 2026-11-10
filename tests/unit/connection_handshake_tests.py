"""Security regression tests for the AMQP connection handshake state machine.

The rule under test is that recognizing a method class does not make the method legal at the
current handshake stage. Every case enters through `Connection._on_data_available`, the same entry
point the adapters use, with frames encoded and decoded over the real wire path rather than
dispatched as direct method calls.
"""

import unittest
from unittest import mock

from pika import connection, credentials, exceptions, frame, spec
from pika._utils import override


class RecordingConnection(connection.Connection):
    """Connection double that records emitted wire frames and user callback invocations.

    The transport is fully in-memory: frames written through `_adapter_emit_data` are buffered,
    and stream teardown records the reason instead of touching a socket.
    """

    def __init__(self, parameters=None):
        self.emitted_frames = []
        self.disconnect_reasons = []
        self.on_open_calls = 0
        self.on_open_error_calls = []
        self.on_close_calls = []
        # Every frame a registered channel receives lands here, so a test can
        # prove a failed handshake never delivers user content.
        self.delivered_frames = []

        super().__init__(parameters=parameters,
                         on_open_callback=lambda _conn: self._record_open(),
                         on_open_error_callback=lambda _conn, error: self.
                         on_open_error_calls.append(error),
                         on_close_callback=lambda _conn, error: self.
                         on_close_calls.append(error),
                         internal_connection_workflow=False)

        # A real adapter enters the PROTOCOL stage once the stream is up and
        # the protocol header has been sent; handshake frames are illegal before.
        self._on_stream_connected()

    def register_channel_stub(self, channel_number):
        """Register a channel double that records whatever frames it receives."""
        channel_stub = mock.Mock()
        channel_stub.channel_number = channel_number

        def record(frame_value):
            self.delivered_frames.append(frame_value)

        channel_stub._handle_content_frame.side_effect = record
        self._channels[channel_number] = channel_stub
        return channel_stub

    def _record_open(self):
        self.on_open_calls += 1

    @override
    def _adapter_connect_stream(self):
        raise NotImplementedError

    @override
    def _adapter_disconnect_stream(self):
        # Mirror the adapter contract: teardown is asynchronous, completed by
        # `_on_stream_terminated` on the next loop turn.
        reason = self._error
        self.disconnect_reasons.append(reason)
        self._on_stream_terminated(None)

    @override
    def _adapter_call_later(self, delay, callback):
        return None

    @override
    def _adapter_remove_timeout(self, timeout_id):
        return None

    @override
    def _adapter_add_callback_threadsafe(self, callback):
        callback()

    @override
    def _adapter_emit_data(self, data):
        self.emitted_frames.append(data)

    def receive(self, *frames):
        """Feed marshaled frames through the real data entry point."""
        self._on_data_available(b''.join(each.marshal() for each in frames))

    def emit(self):
        """Decode every frame the connection has sent back to the broker."""
        decoded = []
        for chunk in self.emitted_frames:
            consumed, value = frame.decode_frame(bytearray(chunk))
            assert consumed == len(chunk)
            decoded.append(value)
        return decoded

    def emitted_methods(self):
        return [
            value.method
            for value in self.emit()
            if isinstance(value, frame.Method)
        ]


def make_start(mechanisms='PLAIN',
               server_properties=None,
               version_major=0,
               version_minor=9):
    return frame.Method(
        0,
        spec.Connection.Start(
            version_major=version_major,
            version_minor=version_minor,
            server_properties=server_properties if server_properties is not None
            else {'capabilities': {
                'basic.nack': True
            }},
            mechanisms=mechanisms))


def make_tune(channel_max=0, frame_max=0, heartbeat=0):
    return frame.Method(
        0,
        spec.Connection.Tune(channel_max=channel_max,
                             frame_max=frame_max,
                             heartbeat=heartbeat))


def make_open_ok():
    return frame.Method(0, spec.Connection.OpenOk(known_hosts=''))


def make_broker_close(reply_code=403, reply_text='access refused'):
    return frame.Method(
        0,
        spec.Connection.Close(reply_code=reply_code,
                              reply_text=reply_text,
                              class_id=0,
                              method_id=0))


class HandshakeTestBase(unittest.TestCase):

    def setUp(self):
        self.conn = RecordingConnection()

    def tearDown(self):
        del self.conn

    def assert_no_user_visible_delivery(self):
        self.assertEqual(self.conn.delivered_frames, [])
        self.assertEqual(self.conn.on_open_calls, 0)

    def assert_handshake_terminated(self):
        # The protocol error terminates the stream; once termination completes
        # the connection is CLOSED. Termination re-registers the Start handler
        # for a potential reconnect, so the per-handler state guard, not an
        # absent callback, is what stops a late frame restarting the handshake.
        self.assertTrue(self.conn.is_closed)
        self.assertEqual(self.conn.on_open_calls, 0)
        self.assertEqual(self.conn.delivered_frames, [])


class NormalNegotiationTests(HandshakeTestBase):

    def test_full_handshake_emits_startok_tuneok_open_and_fires_on_open(self):
        conn = self.conn

        conn.receive(make_start())
        self.assertEqual(conn.connection_state, conn.CONNECTION_START)
        conn.receive(make_tune())
        self.assertEqual(conn.connection_state, conn.CONNECTION_TUNE)
        conn.receive(make_open_ok())

        methods = conn.emitted_methods()
        self.assertEqual([type(method) for method in methods], [
            spec.Connection.StartOk,
            spec.Connection.TuneOk,
            spec.Connection.Open,
        ])

        self.assertTrue(conn.is_open)
        self.assertTrue(conn._opened)
        self.assertEqual(conn.on_open_calls, 1)
        self.assertEqual(conn.on_open_error_calls, [])
        self.assertEqual(conn.on_close_calls, [])

        # A second OpenOk cannot fire the user callback a second time, because
        # the one-shot OpenOk handler was removed when the first one ran.
        conn.receive(make_open_ok())
        self.assertEqual(conn.on_open_calls, 1)

    def test_on_open_only_runs_after_open_ok_not_after_tune(self):
        conn = self.conn
        conn.receive(make_start())
        conn.receive(make_tune())

        self.assertEqual(conn.on_open_calls, 0)
        self.assertFalse(conn.is_open)
        self.assertEqual([type(method) for method in conn.emitted_methods()], [
            spec.Connection.StartOk, spec.Connection.TuneOk,
            spec.Connection.Open
        ])


class PlainCredentialsTests(HandshakeTestBase):

    def test_plain_startok_carries_plain_mechanism_and_response(self):
        conn = RecordingConnection(
            connection.ConnectionParameters(
                credentials=credentials.PlainCredentials('alice', 's3cret')))

        conn.receive(make_start(mechanisms='PLAIN AMQPLAIN'))

        start_ok = conn.emitted_methods()[0]
        self.assertIsInstance(start_ok, spec.Connection.StartOk)
        self.assertEqual(start_ok.mechanism, 'PLAIN')
        self.assertEqual(start_ok.response, b'\0alice\0s3cret')

    def test_plain_credentials_rejected_when_server_omits_plain(self):
        conn = RecordingConnection(
            connection.ConnectionParameters(
                credentials=credentials.PlainCredentials('alice', 's3cret')))

        conn.receive(make_start(mechanisms='EXTERNAL'))

        # No StartOk is emitted and the selected PLAIN mechanism is never
        # silently swapped for EXTERNAL.
        self.assertEqual(conn.emitted_methods(), [])
        self.assertFalse(conn.is_open)
        self.assert_no_user_visible_delivery()

        [error] = conn.on_open_error_calls
        self.assertIs(type(error), exceptions.AuthenticationError)
        self.assertEqual(error.args[0], credentials.PlainCredentials.TYPE)
        self.assertTrue(conn.is_closed)

    def test_erase_on_connect_wipes_plain_secret_after_use(self):
        creds = credentials.PlainCredentials('alice',
                                             's3cret',
                                             erase_on_connect=True)
        conn = RecordingConnection(
            connection.ConnectionParameters(credentials=creds))
        # Parameters deep-copy the credentials object, so the erased instance is
        # the one held by the connection, not the caller's original.
        held_credentials = conn.params.credentials

        conn.receive(make_start(mechanisms='PLAIN'))

        self.assertIsNone(held_credentials.username)
        self.assertIsNone(held_credentials.password)
        start_ok = conn.emitted_methods()[0]
        self.assertEqual(start_ok.mechanism, 'PLAIN')
        self.assertEqual(start_ok.response, b'\0alice\0s3cret')


class ExternalCredentialsTests(HandshakeTestBase):

    def test_external_startok_carries_external_mechanism_empty_response(self):
        conn = RecordingConnection(
            connection.ConnectionParameters(
                credentials=credentials.ExternalCredentials()))

        conn.receive(make_start(mechanisms='PLAIN EXTERNAL'))

        start_ok = conn.emitted_methods()[0]
        self.assertEqual(start_ok.mechanism, 'EXTERNAL')
        self.assertEqual(start_ok.response, b'')

    def test_external_credentials_rejected_when_server_omits_external(self):
        conn = RecordingConnection(
            connection.ConnectionParameters(
                credentials=credentials.ExternalCredentials()))

        conn.receive(make_start(mechanisms='PLAIN'))

        self.assertEqual(conn.emitted_methods(), [])
        self.assertFalse(conn.is_open)
        self.assert_no_user_visible_delivery()

        [error] = conn.on_open_error_calls
        self.assertIs(type(error), exceptions.AuthenticationError)
        self.assertEqual(error.args[0], credentials.ExternalCredentials.TYPE)
        self.assertTrue(conn.is_closed)

    def test_external_never_silently_falls_back_to_plain(self):
        # Even though PLAIN is offered, an EXTERNAL credentials object must not
        # answer with PLAIN; the negotiation has to fail instead.
        conn = RecordingConnection(
            connection.ConnectionParameters(
                credentials=credentials.ExternalCredentials()))

        conn.receive(make_start(mechanisms='PLAIN'))

        self.assertEqual([type(method) for method in conn.emitted_methods()],
                         [])
        [error] = conn.on_open_error_calls
        self.assertEqual(error.args[0], 'EXTERNAL')


class DuplicateStartTests(HandshakeTestBase):

    def test_second_start_after_start_is_protocol_violation(self):
        conn = self.conn
        conn.receive(make_start())
        first_start_ok_count = len(conn.emitted_methods())

        conn.receive(make_start(mechanisms='PLAIN'))

        # The duplicate must not drive a second StartOk or any state change.
        self.assertEqual(len(conn.emitted_methods()), first_start_ok_count)
        self.assert_no_user_visible_delivery()
        self._assert_unexpected_frame_error(spec.Connection.Start)

    def test_second_start_during_tune_is_protocol_violation(self):
        conn = self.conn
        conn.receive(make_start())
        conn.receive(make_tune())

        conn.receive(make_start(mechanisms='PLAIN'))

        self.assert_no_user_visible_delivery()
        self._assert_unexpected_frame_error(spec.Connection.Start)

    def test_start_after_open_is_protocol_violation(self):
        conn = self.conn
        conn.receive(make_start())
        conn.receive(make_tune())
        conn.receive(make_open_ok())
        self.assertEqual(conn.on_open_calls, 1)

        conn.receive(make_start(mechanisms='PLAIN'))

        # The post-open violation closes an already-open connection, so it goes
        # through on_close, never through a second on_open.
        self.assertEqual(conn.on_open_calls, 1)
        self._assert_unexpected_frame_error(spec.Connection.Start,
                                            open_connection=True)

    def _assert_unexpected_frame_error(self, method_cls, open_connection=False):
        conn = self.conn
        self.assertEqual(len(conn.disconnect_reasons), 1)
        reason = conn.disconnect_reasons[0]
        self.assertIs(type(reason), exceptions.UnexpectedFrameError)
        self.assertIsInstance(reason, exceptions.ProtocolSyntaxError)
        self.assertEqual(type(reason.args[0].method), method_cls)
        self.assertTrue(conn.is_closed)
        self.assertEqual(conn.delivered_frames, [])

        if open_connection:
            [closed_error] = conn.on_close_calls
            self.assertIs(closed_error, reason)
            self.assertEqual(conn.on_open_error_calls, [])
            # The single on_open from the completed handshake stays at one.
            self.assertEqual(conn.on_open_calls, 1)
        else:
            [open_error] = conn.on_open_error_calls
            self.assertIs(open_error, reason)
            self.assertEqual(conn.on_close_calls, [])
            self.assertEqual(conn.on_open_calls, 0)


class EarlyOpenOkTests(HandshakeTestBase):

    def test_open_ok_before_start_is_protocol_violation(self):
        self.conn.receive(make_open_ok())
        self._assert_early_open_ok_rejected()

    def test_open_ok_during_start_is_protocol_violation(self):
        self.conn.receive(make_start())
        self.conn.receive(make_open_ok())
        self._assert_early_open_ok_rejected()

    def test_open_ok_during_tune_before_our_open_is_protocol_violation(self):
        # The OpenOk callback only exists once Connection.Open has been sent.
        # Remove it to model the window before that RPC registers its reply
        # handler: an OpenOk arriving then has no pending callback and must not
        # be mistaken for our RPC reply.
        conn = self.conn
        conn.receive(make_start())
        conn.receive(make_tune())
        self.assertEqual(conn.connection_state, conn.CONNECTION_TUNE)
        conn.callbacks.remove(0, spec.Connection.OpenOk)
        conn.emitted_frames.clear()

        conn.receive(make_open_ok())

        self.assertEqual(conn.emitted_methods(), [])
        self.assertFalse(conn.is_open)
        self.assert_no_user_visible_delivery()
        self._assert_unexpected_frame(spec.Connection.OpenOk)

    def _assert_early_open_ok_rejected(self):
        conn = self.conn
        self.assertFalse(conn.is_open)
        self.assert_no_user_visible_delivery()
        self._assert_unexpected_frame(spec.Connection.OpenOk)

    def _assert_unexpected_frame(self, method_cls):
        conn = self.conn
        self.assertEqual(len(conn.disconnect_reasons), 1)
        reason = conn.disconnect_reasons[0]
        self.assertIs(type(reason), exceptions.UnexpectedFrameError)
        self.assertEqual(type(reason.args[0].method), method_cls)
        self.assertEqual(conn.on_open_error_calls, [reason])
        self.assertEqual(conn.on_close_calls, [])
        self.assertTrue(conn.is_closed)


class ChannelDataDuringHandshakeTests(HandshakeTestBase):

    def test_channel_method_frame_during_start_ends_connection(self):
        # A channel stub exists, so the rejection is about handshake state, not
        # an absent channel; the stub must never receive the frame.
        self.conn.register_channel_stub(1)
        self.conn.receive(make_start())
        self.conn.receive(frame.Method(1, spec.Channel.OpenOk()))
        self._assert_channel_frame_rejected(spec.Channel.OpenOk)

    def test_channel_content_header_during_tune_ends_connection(self):
        conn = self.conn
        conn.register_channel_stub(1)
        conn.receive(make_start())
        conn.receive(make_tune())

        conn.receive(frame.Header(1, 0, spec.BasicProperties()))

        self.assert_no_user_visible_delivery()
        self._assert_channel_frame_rejected(spec.BasicProperties)

    def test_channel_body_frame_before_open_ends_connection(self):
        self.conn.register_channel_stub(1)
        self.conn.receive(frame.Body(1, b'publishing before open'))
        self._assert_channel_frame_rejected(None)

    def test_registered_channel_receives_frames_only_after_open(self):
        # The positive counterpart to the handshake rejections: once open, a
        # frame for a registered channel is delivered to that channel.
        conn = self.conn
        channel_stub = conn.register_channel_stub(1)
        conn.receive(make_start())
        conn.receive(make_tune())
        conn.receive(make_open_ok())

        deliver = frame.Method(
            1, spec.Basic.Deliver('ctag', 1, exchange='', routing_key='q'))
        conn.receive(deliver)

        channel_stub._handle_content_frame.assert_called_once_with(deliver)
        self.assertEqual(conn.delivered_frames, [deliver])
        self.assertEqual(conn.disconnect_reasons, [])

    def test_channel_frame_on_unregistered_channel_after_open_is_rejected(self):
        # Even after OPEN, a frame for a channel that was never created cannot
        # be delivered and must not be swallowed by the critical log alone.
        conn = self.conn
        conn.receive(make_start())
        conn.receive(make_tune())
        conn.receive(make_open_ok())

        conn.receive(frame.Method(7, spec.Channel.OpenOk()))

        self.assertEqual(conn.on_open_calls, 1)
        self.assertEqual(len(conn.disconnect_reasons), 1)
        reason = conn.disconnect_reasons[0]
        self.assertIs(type(reason), exceptions.UnexpectedFrameError)
        self.assertEqual(reason.args[0].channel_number, 7)
        self.assertEqual(conn.on_close_calls, [reason])
        self.assertEqual(conn.delivered_frames, [])

    def _assert_channel_frame_rejected(self, method_cls):
        conn = self.conn
        self.assertEqual(len(conn.disconnect_reasons), 1)
        reason = conn.disconnect_reasons[0]
        self.assertIs(type(reason), exceptions.UnexpectedFrameError)
        self.assertIsInstance(reason, exceptions.ProtocolSyntaxError)
        self.assertEqual(reason.args[0].channel_number, 1)
        if method_cls is not None:
            offending = reason.args[0]
            if isinstance(offending, frame.Method):
                self.assertEqual(type(offending.method), method_cls)
            else:
                self.assertIsInstance(offending, frame.Header)
        self.assertEqual(conn.on_open_error_calls, [reason])
        self.assertEqual(conn.delivered_frames, [])
        self.assertTrue(conn.is_closed)


class BrokerCloseDuringHandshakeTests(HandshakeTestBase):

    def test_broker_close_during_start_is_classified_as_broker_close(self):
        conn = self.conn
        conn.receive(make_start())
        emitted_after_start = len(conn.emitted_methods())

        conn.receive(
            make_broker_close(reply_code=403, reply_text='access refused'))

        self.assertEqual(len(conn.emitted_methods()), emitted_after_start)
        # A legal Connection.Close is not a protocol violation. The recorded
        # teardown reason keeps its concrete broker-close type; during the START
        # stage the user-facing error is heuristically wrapped as a probable
        # authentication failure with the broker close as its __cause__.
        self.assertEqual(len(conn.disconnect_reasons), 1)
        recorded = conn.disconnect_reasons[0]
        self.assertIs(type(recorded), exceptions.ConnectionClosedByBroker)
        self.assertNotIsInstance(recorded, exceptions.UnexpectedFrameError)
        self.assertEqual(recorded.reply_code, 403)
        self.assertEqual(recorded.reply_text, 'access refused')

        [reason] = conn.on_open_error_calls
        self.assertIs(type(reason), exceptions.ProbableAuthenticationError)
        self.assertIs(reason.__cause__, recorded)
        self.assertEqual(conn.on_close_calls, [])
        self.assertEqual(conn.on_open_calls, 0)
        self.assertTrue(conn.is_closed)

    def test_broker_close_during_tune_does_not_complete_open(self):
        conn = self.conn
        conn.receive(make_start())
        conn.receive(make_tune())

        conn.receive(make_broker_close(reply_code=530,
                                       reply_text='not allowed'))

        self.assertFalse(conn.is_open)
        self.assertEqual(conn.on_open_calls, 0)
        [reason] = conn.on_open_error_calls
        self.assertIs(type(reason), exceptions.ProbableAccessDeniedError)
        self.assertIs(type(reason.__cause__),
                      exceptions.ConnectionClosedByBroker)
        self.assertEqual(reason.__cause__.reply_code, 530)
        self.assertIs(conn.disconnect_reasons[0], reason.__cause__)

    def test_broker_close_after_open_uses_on_close_callback(self):
        conn = self.conn
        conn.receive(make_start())
        conn.receive(make_tune())
        conn.receive(make_open_ok())

        conn.receive(make_broker_close(reply_code=320, reply_text='forced'))

        self.assertEqual(conn.on_open_calls, 1)
        self.assertEqual(conn.on_open_error_calls, [])
        [reason] = conn.on_close_calls
        # Once open, no pre-open heuristic applies: the broker close is
        # delivered with its concrete ConnectionClosedByBroker type.
        self.assertIs(type(reason), exceptions.ConnectionClosedByBroker)
        self.assertNotIsInstance(reason, exceptions.UnexpectedFrameError)
        self.assertEqual(reason.reply_code, 320)


class ClientAbortDuringHandshakeTests(HandshakeTestBase):

    def test_client_close_during_start_aborts_without_open_callback(self):
        conn = self.conn
        conn.receive(make_start())

        conn.close(reply_code=200, reply_text='giving up')

        self.assertEqual(conn.connection_state, conn.CONNECTION_CLOSED)
        self.assertFalse(conn._opened)
        self.assertEqual(conn.on_open_calls, 0)
        [reason] = conn.on_open_error_calls
        self.assertIs(type(reason), exceptions.ConnectionOpenAborted)


class StaleFramesAfterTerminationTests(HandshakeTestBase):

    def test_frames_arriving_after_violation_do_not_revive_handlers(self):
        conn = self.conn
        conn.receive(make_start())

        # One violation ends the handshake.
        conn.receive(make_open_ok())
        self.assertTrue(conn.is_closed)
        [original_error] = conn.on_open_error_calls

        # Frames that would normally advance the handshake must not resurrect
        # any handler or mutate state after termination. The re-registered
        # Start handler rejects the CLOSED state by raising; frames without a
        # handler hit the frame-level guard and terminate again. Neither path
        # may run a handshake step.
        for stale_frame in (make_tune(), make_start(), make_open_ok()):
            try:
                conn.receive(stale_frame)
            except exceptions.UnexpectedFrameError:
                pass
            self.assertTrue(conn.is_closed)
            self.assertFalse(conn._opened)
            self.assertEqual(conn.on_open_calls, 0)

        # Every rejection reports an UnexpectedFrameError; the original reason
        # stays first, and no frame re-runs a handshake step or opens the
        # connection.
        self.assertEqual(conn.on_open_error_calls[0], original_error)
        self.assertTrue(
            all(
                isinstance(error, exceptions.UnexpectedFrameError)
                for error in conn.on_open_error_calls))
        self.assertEqual(conn.on_close_calls, [])
        self.assertEqual(conn.delivered_frames, [])

    def test_frames_trailing_a_violation_in_one_buffer_are_abandoned(self):
        conn = self.conn
        conn.register_channel_stub(1)
        conn.receive(make_start())

        # The violation is not the last frame in the data event. Decoding must
        # stop at it, leaving the heartbeat and channel frame unprocessed.
        buffered = (make_open_ok().marshal() + frame.Heartbeat().marshal() +
                    frame.Method(1, spec.Channel.OpenOk()).marshal())
        conn._on_data_available(buffered)

        self.assertTrue(conn.is_closed)
        self.assertEqual(len(conn.disconnect_reasons), 1)
        self.assertIs(type(conn.disconnect_reasons[0]),
                      exceptions.UnexpectedFrameError)
        self.assertEqual(conn.delivered_frames, [])
        self.assertEqual(conn.on_open_calls, 0)
        self.assertEqual(bytearray(), conn._frame_buffer)


if __name__ == '__main__':
    unittest.main()
