"""Tests for pika.adapters.blocking_connection.BlockingChannel."""

import threading
import unittest
from collections import deque
from unittest import mock

from pika import channel, exceptions, frame, spec
from pika.adapters import blocking_connection

BLOCKING_CHANNEL = 'pika.adapters.blocking_connection.BlockingChannel'
BLOCKING_CONNECTION = 'pika.adapters.blocking_connection.BlockingConnection'


class ChannelTemplate(channel.Channel):
    channel_number = 1


class BlockingChannelTests(unittest.TestCase):

    @mock.patch(BLOCKING_CONNECTION)
    def _create_connection(self, connection=None):
        return connection

    def setUp(self):
        self.connection = self._create_connection()
        self.channel_impl_mock = mock.Mock(spec=ChannelTemplate,
                                           is_closing=False,
                                           is_closed=False,
                                           is_open=True)
        self.obj = blocking_connection.BlockingChannel(self.channel_impl_mock,
                                                       self.connection)

    def tearDown(self):
        del self.connection
        del self.obj

    def test_init_initial_value_confirmation(self):
        self.assertFalse(self.obj._delivery_confirmation)

    def test_init_initial_value_pending_events(self):
        self.assertEqual(self.obj._pending_events, deque())

    def test_init_initial_value_buback_return(self):
        self.assertIsNone(self.obj._puback_return)

    def test_basic_consume_legacy_parameter_queue(self):
        # This is for the unlikely scenario where only
        # the first parameter is updated
        with self.assertRaises(TypeError):
            self.obj.basic_consume('queue', 'whoops this should be a callback')

    def test_basic_consume_legacy_parameter_callback(self):
        with self.assertRaises(TypeError):
            self.obj.basic_consume(mock.Mock(), 'queue')

    def test_queue_declare_legacy_parameter_callback(self):
        with self.assertRaises(TypeError):
            self.obj.queue_declare(mock.Mock(), 'queue')

    def test_exchange_declare_legacy_parameter_callback(self):
        with self.assertRaises(TypeError):
            self.obj.exchange_declare(mock.Mock(), 'exchange')

    def test_queue_bind_legacy_parameter_callback(self):
        with self.assertRaises(TypeError):
            self.obj.queue_bind(mock.Mock(), 'queue', 'exchange')

    def _broker_cancelled_consumer(self, auto_ack):
        info = blocking_connection._ConsumerInfo(
            'ctag', auto_ack=auto_ack, on_message_callback=mock.Mock())
        info.state = blocking_connection._ConsumerInfo.CANCELLED_BY_BROKER
        self.obj._consumer_infos['ctag'] = info
        self.channel_impl_mock._consumers = {}
        self.channel_impl_mock.channel_number = 1
        self.obj._pending_events.extend([
            blocking_connection._ConsumerDeliveryEvt(
                spec.Basic.Deliver(consumer_tag='ctag', delivery_tag=7),
                spec.BasicProperties(), b''),
            blocking_connection._ConsumerCancellationEvt(
                frame.Method(1, spec.Basic.Cancel(consumer_tag='ctag'))),
        ])

    def test_basic_cancel_does_not_cancel_a_broker_cancelled_consumer_again(
            self):
        self._broker_cancelled_consumer(auto_ack=False)
        events = mock.Mock()
        events.attach_mock(self.channel_impl_mock.basic_reject, 'reject')
        events.attach_mock(self.connection._flush_output, 'flush')

        self.assertEqual(self.obj.basic_cancel('ctag'), [])

        self.channel_impl_mock.basic_cancel.assert_not_called()
        self.assertEqual([c[0] for c in events.mock_calls], ['reject', 'flush'])
        # Waiting on anything but "always ready" would never end: no reply is coming.
        self.assertEqual(self.connection._flush_output.call_args[0][1:],
                         self.obj._ALWAYS_READY_WAITERS)
        self.assertEqual(len(self.obj._pending_events), 0)

    def test_basic_cancel_returns_a_broker_cancelled_auto_ack_consumers_messages(
            self):
        self._broker_cancelled_consumer(auto_ack=True)

        messages = self.obj.basic_cancel('ctag')

        self.channel_impl_mock.basic_cancel.assert_not_called()
        self.assertEqual([m[0].delivery_tag for m in messages], [7])
        self.assertEqual(len(self.obj._pending_events), 0)

    def test_basic_cancel_of_a_broker_cancelled_consumer_on_a_closing_channel_raises(
            self):
        self._broker_cancelled_consumer(auto_ack=True)
        self.channel_impl_mock._raise_if_not_open.side_effect = (
            exceptions.ChannelWrongStateError('Channel is closing.'))

        with self.assertRaises(exceptions.ChannelWrongStateError):
            self.obj.basic_cancel('ctag')

    def test_basic_cancel_flushes_rejects_for_deliveries_during_the_cancel(
            self):
        self.obj._consumer_infos['ctag'] = blocking_connection._ConsumerInfo(
            'ctag', auto_ack=False, on_message_callback=mock.Mock())
        self.obj._consumer_infos['ctag'].state = (
            blocking_connection._ConsumerInfo.ACTIVE)
        self.channel_impl_mock._consumers = {'ctag': mock.Mock()}
        self.channel_impl_mock.channel_number = 1

        def deliver_while_cancelling(**_kwargs):
            self.obj._pending_events.append(
                blocking_connection._ConsumerDeliveryEvt(
                    spec.Basic.Deliver(consumer_tag='ctag', delivery_tag=7),
                    spec.BasicProperties(), b''))

        self.channel_impl_mock.basic_cancel.side_effect = deliver_while_cancelling
        events = mock.Mock()
        events.attach_mock(self.channel_impl_mock.basic_reject, 'reject')
        events.attach_mock(self.connection._flush_output, 'flush')

        self.assertEqual(self.obj.basic_cancel('ctag'), [])

        self.assertEqual([c[0] for c in events.mock_calls],
                         ['flush', 'reject', 'flush'])

    def test_basic_cancel_legacy_parameter(self):
        with self.assertRaises(TypeError):
            self.obj.basic_cancel(mock.Mock(), 'tag')

    def test_basic_get_legacy_parameter(self):
        with self.assertRaises(TypeError):
            self.obj.basic_get(mock.Mock())

    def test_basic_consume(self):
        with mock.patch.object(self.obj._impl, '_generate_consumer_tag'):
            self.channel_impl_mock._generate_consumer_tag.return_value = 'ctag0'
            self.channel_impl_mock.basic_consume.return_value = 'ctag0'

            self.obj.basic_consume('queue', mock.Mock())

            self.assertEqual(self.obj._consumer_infos['ctag0'].state,
                             blocking_connection._ConsumerInfo.ACTIVE)

    def test_context_manager(self):
        with self.obj:
            self.assertFalse(self.channel_impl_mock.close.called)
        self.channel_impl_mock.close.assert_called_once_with(
            reply_code=0, reply_text='Normal shutdown')

    def test_context_manager_does_not_suppress_exception(self):

        class TestException(Exception):
            pass

        with self.assertRaises(TestException), self.obj:
            self.assertFalse(self.channel_impl_mock.close.called)
            raise TestException()
        self.channel_impl_mock.close.assert_called_once_with(
            reply_code=0, reply_text='Normal shutdown')

    def test_context_manager_exit_with_closed_channel(self):
        with self.obj as chan:
            self.assertFalse(self.channel_impl_mock.close.called)
            chan.close()
        self.channel_impl_mock.close.assert_called_with(
            reply_code=0, reply_text='Normal shutdown')

    def test_consumer_tags_property(self):
        with mock.patch.object(self.obj._impl, '_generate_consumer_tag'):
            self.assertEqual(0, len(self.obj.consumer_tags))
            self.channel_impl_mock._generate_consumer_tag.return_value = 'ctag0'
            self.channel_impl_mock.basic_consume.return_value = 'ctag0'
            self.obj.basic_consume('queue', mock.Mock())
            self.assertEqual(1, len(self.obj.consumer_tags))
            self.assertIn('ctag0', self.obj.consumer_tags)


class _ScriptedIoloop:
    """Ioloop stub running one scripted broker action per `poll()`.

    The real `BlockingConnection._flush_output` loop polls until a waiter is
    ready, so each scripted step stands in for broker frames arriving from
    the socket. Polling past the end of the script fails the test instead of
    hanging.
    """

    def __init__(self):
        self.steps = deque()

    def poll(self):
        assert self.steps, 'flush loop polled with no scripted broker event'
        self.steps.popleft()()

    def process_timeouts(self):
        pass


class BlockingChannelPubackTests(unittest.TestCase):
    """`BlockingChannel.basic_publish` in publisher-acknowledgments mode.

    Exercises the real `_flush_output` wait and callback dispatch: the
    `BlockingChannel` and `BlockingConnection` are real, only the impl
    channel, impl connection and ioloop are stubs, and broker frames arrive
    through the callbacks the adapter registered.
    """

    def setUp(self):
        self.ioloop = _ScriptedIoloop()
        self.connection_impl_mock = mock.Mock(is_closed=False, is_open=True)
        self.connection_impl_mock._transport = mock.Mock()
        self.connection_impl_mock._get_write_buffer_size.return_value = 0
        self.connection_impl_mock.ioloop = self.ioloop

        self.connection = object.__new__(blocking_connection.BlockingConnection)
        self.connection._cleanup_mutex = threading.Lock()
        self.connection._event_dispatch_suspend_depth = 0
        self.connection._ready_events = deque()
        self.connection._channels_pending_dispatch = set()
        self.connection._closed_result = blocking_connection._CallbackResult(
            blocking_connection.BlockingConnection._OnClosedArgs)
        self.connection._server_channel_closures = deque()
        self.connection._impl = self.connection_impl_mock

        self.channel_impl_mock = mock.Mock(spec=ChannelTemplate,
                                           channel_number=1,
                                           is_closing=False,
                                           is_closed=False,
                                           is_open=True)
        self.channel_impl_mock.add_on_close_callback.side_effect = (
            lambda callback: setattr(self, '_close_callback', callback))
        self.channel_impl_mock.add_on_return_callback.side_effect = (
            lambda callback: setattr(self, '_return_callback', callback))

        self.obj = blocking_connection.BlockingChannel(self.channel_impl_mock,
                                                       self.connection)

    def tearDown(self):
        del self.obj
        del self.connection

    def _enable_confirm(self):

        def confirm_delivery(ack_nack_callback, callback):
            self._ack_nack_callback = ack_nack_callback
            callback()

        self.channel_impl_mock.confirm_delivery.side_effect = confirm_delivery
        self.obj.confirm_delivery()

    def _broker_acks(self, delivery_tag):
        self._ack_nack_callback(frame.Method(1, spec.Basic.Ack(delivery_tag)))

    def _broker_nacks(self, delivery_tag):
        self._ack_nack_callback(frame.Method(1, spec.Basic.Nack(delivery_tag)))

    def _broker_returns(self, properties, body):
        method = spec.Basic.Return(reply_code=312,
                                   reply_text='NO_ROUTE',
                                   exchange='exchange',
                                   routing_key='routing.key')
        self._return_callback(self.channel_impl_mock, method, properties, body)

    def _broker_closes_channel(self):
        self.channel_impl_mock.is_closed = True
        self.channel_impl_mock.is_open = False
        self.channel_impl_mock._raise_if_not_open.side_effect = (
            exceptions.ChannelWrongStateError('Channel is closed.'))
        self._close_callback(
            self.channel_impl_mock,
            exceptions.ChannelClosedByBroker(404, 'NOT_FOUND - no queue'))

    def test_return_then_ack_raises_unroutable_with_own_message(self):
        self._enable_confirm()
        properties = spec.BasicProperties(content_type='text/plain',
                                          message_id='msg-1')
        body = b'payload-1'
        self.ioloop.steps += [
            lambda: self._broker_returns(properties, body),
            lambda: self._broker_acks(1),
        ]

        with self.assertRaises(exceptions.UnroutableError) as ctx:
            self.obj.basic_publish('exchange',
                                   'routing.key',
                                   body,
                                   properties=properties,
                                   mandatory=True)

        self.assertEqual(1, len(ctx.exception.messages))
        returned = ctx.exception.messages[0]
        self.assertIs(returned.body, body)
        self.assertIs(returned.properties, properties)
        self.assertEqual(312, returned.method.reply_code)

    def test_return_then_nack_raises_nack_with_returned_message(self):
        self._enable_confirm()
        properties = spec.BasicProperties(content_type='text/plain',
                                          message_id='msg-1')
        body = b'payload-1'
        self.ioloop.steps += [
            lambda: self._broker_returns(properties, body),
            lambda: self._broker_nacks(1),
        ]

        with self.assertRaises(exceptions.NackError) as ctx:
            self.obj.basic_publish('exchange',
                                   'routing.key',
                                   body,
                                   properties=properties,
                                   mandatory=True)

        self.assertEqual(1, len(ctx.exception.messages))
        returned = ctx.exception.messages[0]
        self.assertIs(returned.body, body)
        self.assertIs(returned.properties, properties)

    def test_nack_without_return_raises_nack_without_messages(self):
        self._enable_confirm()
        self.ioloop.steps.append(lambda: self._broker_nacks(1))

        with self.assertRaises(exceptions.NackError) as ctx:
            self.obj.basic_publish(
                'exchange',
                'routing.key',
                b'payload-1',
                properties=spec.BasicProperties(message_id='msg-1'),
                mandatory=True)

        self.assertEqual([], ctx.exception.messages)

    def test_failure_leaves_nothing_for_next_publish(self):
        self._enable_confirm()
        properties = spec.BasicProperties(message_id='msg-1')
        body = b'payload-1'
        self.ioloop.steps += [
            lambda: self._broker_returns(properties, body),
            lambda: self._broker_acks(1),
        ]
        with self.assertRaises(exceptions.UnroutableError) as ctx:
            self.obj.basic_publish('exchange',
                                   'routing.key',
                                   body,
                                   properties=properties,
                                   mandatory=True)
        self.assertIs(ctx.exception.messages[0].body, body)

        self.ioloop.steps.append(lambda: self._broker_acks(2))
        self.assertIsNone(
            self.obj.basic_publish(
                'exchange',
                'routing.key',
                b'payload-2',
                properties=spec.BasicProperties(message_id='msg-2'),
                mandatory=True))

        # A success must not leave anything behind either.
        self.ioloop.steps.append(lambda: self._broker_acks(3))
        self.assertIsNone(
            self.obj.basic_publish(
                'exchange',
                'routing.key',
                b'payload-3',
                properties=spec.BasicProperties(message_id='msg-3'),
                mandatory=True))

    def test_channel_close_while_waiting_for_confirmation(self):
        self._enable_confirm()
        properties = spec.BasicProperties(message_id='msg-1')
        body = b'payload-1'
        self.ioloop.steps += [
            lambda: self._broker_returns(properties, body),
            self._broker_closes_channel,
        ]

        with self.assertRaises(exceptions.ChannelClosedByBroker):
            self.obj.basic_publish('exchange',
                                   'routing.key',
                                   body,
                                   properties=properties,
                                   mandatory=True)

        # The interrupted publish's returned message must not surface in a
        # later call.
        with self.assertRaises(exceptions.ChannelWrongStateError):
            self.obj.basic_publish(
                'exchange',
                'routing.key',
                b'payload-2',
                properties=spec.BasicProperties(message_id='msg-2'),
                mandatory=True)

    def test_duplicate_confirmation_does_not_end_wait(self):
        self._enable_confirm()
        self.ioloop.steps.append(lambda: self._broker_acks(1))
        self.assertIsNone(
            self.obj.basic_publish(
                'exchange',
                'routing.key',
                b'payload-1',
                properties=spec.BasicProperties(message_id='msg-1'),
                mandatory=True))

        # A retransmitted confirmation for delivery-tag 1 must not complete
        # the next publish's wait; only delivery-tag 2 may.
        properties = spec.BasicProperties(message_id='msg-2')
        body = b'payload-2'
        self.ioloop.steps += [
            lambda: self._broker_acks(1),
            lambda: self._broker_returns(properties, body),
            lambda: self._broker_acks(2),
        ]
        with self.assertRaises(exceptions.UnroutableError) as ctx:
            self.obj.basic_publish('exchange',
                                   'routing.key',
                                   body,
                                   properties=properties,
                                   mandatory=True)
        self.assertIs(ctx.exception.messages[0].body, body)

        self.ioloop.steps.append(lambda: self._broker_acks(3))
        self.assertIsNone(
            self.obj.basic_publish(
                'exchange',
                'routing.key',
                b'payload-3',
                properties=spec.BasicProperties(message_id='msg-3'),
                mandatory=True))

    def test_duplicate_nack_does_not_fail_next_publish(self):
        self._enable_confirm()
        self.ioloop.steps.append(lambda: self._broker_nacks(1))
        with self.assertRaises(exceptions.NackError):
            self.obj.basic_publish(
                'exchange',
                'routing.key',
                b'payload-1',
                properties=spec.BasicProperties(message_id='msg-1'),
                mandatory=True)

        self.ioloop.steps += [
            lambda: self._broker_nacks(1),
            lambda: self._broker_acks(2),
        ]
        self.assertIsNone(
            self.obj.basic_publish(
                'exchange',
                'routing.key',
                b'payload-2',
                properties=spec.BasicProperties(message_id='msg-2'),
                mandatory=True))

    def test_without_confirm_mode_publish_does_not_wait(self):
        # The script is empty: any wait for a broker outcome would fail.
        self.assertIsNone(
            self.obj.basic_publish(
                'exchange',
                'routing.key',
                b'payload-1',
                properties=spec.BasicProperties(message_id='msg-1'),
                mandatory=True))
        self.channel_impl_mock.basic_publish.assert_called_once()
