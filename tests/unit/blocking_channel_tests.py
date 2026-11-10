"""Tests for pika.adapters.blocking_connection.BlockingChannel."""

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
        self.channel_impl_mock.callbacks = mock.Mock()
        self.obj = blocking_connection.BlockingChannel(self.channel_impl_mock,
                                                       self.connection)

    def tearDown(self):
        del self.connection
        del self.obj

    def _register_consumer(self,
                           consumer_tag,
                           on_message_callback,
                           auto_ack=False,
                           active=True):
        if not hasattr(self.channel_impl_mock, '_consumers') or not isinstance(
                self.channel_impl_mock._consumers, dict):
            self.channel_impl_mock._consumers = {}
        info = blocking_connection._ConsumerInfo(
            consumer_tag,
            auto_ack=auto_ack,
            on_message_callback=on_message_callback)
        if active:
            info.state = blocking_connection._ConsumerInfo.ACTIVE
        self.obj._consumer_infos[consumer_tag] = info
        self.channel_impl_mock._consumers[consumer_tag] = mock.Mock()
        self.channel_impl_mock.channel_number = 1
        return info

    def _delivery_evt(self, consumer_tag, delivery_tag):
        return blocking_connection._ConsumerDeliveryEvt(
            spec.Basic.Deliver(consumer_tag=consumer_tag,
                               delivery_tag=delivery_tag),
            spec.BasicProperties(), b'body')

    def _cancel_evt(self, consumer_tag):
        return blocking_connection._ConsumerCancellationEvt(
            frame.Method(1, spec.Basic.Cancel(consumer_tag=consumer_tag)))

    def _complete_cancel_ok(self):
        # The mock transport's broker answer: invoke every CancelOk completion
        # callback registered for the tag, as the real CallbackManager would.
        for call in list(self.channel_impl_mock.basic_cancel.call_args_list):
            completion = call.kwargs['callback']
            tag = call.kwargs['consumer_tag']
            completion(frame.Method(1, spec.Basic.CancelOk(consumer_tag=tag)))
            if isinstance(self.channel_impl_mock._consumers, dict):
                self.channel_impl_mock._consumers.pop(tag, None)

    def _broker_cancel(self, consumer_tag):
        self.obj._on_consumer_cancelled_by_broker(
            frame.Method(1, spec.Basic.Cancel(consumer_tag=consumer_tag)))

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

    def test_reused_tag_does_not_dispatch_queued_deliveries_to_new_callback(
            self):
        # The pinned risk: a tag reused after cancel must not hand deliveries
        # queued for the old consumer to the new consumer's callback.
        old_callback = mock.Mock()
        self._register_consumer('ctag', old_callback)
        self.obj._add_pending_event(self._delivery_evt('ctag', 1))
        self.obj._add_pending_event(self._delivery_evt('ctag', 2))

        self.assertEqual(self.obj.basic_cancel('ctag'), [])
        self._complete_cancel_ok()
        self.assertNotIn('ctag', self.obj._consumer_infos)
        self.assertEqual(self.obj._pending_events, deque())
        old_callback.assert_not_called()

        new_callback = mock.Mock()
        self._register_consumer('ctag', new_callback)
        self.obj._add_pending_event(self._delivery_evt('ctag', 3))
        self.obj._dispatch_events()

        old_callback.assert_not_called()
        new_callback.assert_called_once()
        self.assertEqual(
            new_callback.call_args[0][1].delivery_tag, 3,
            'a new delivery must carry its own delivery tag, not an old one')

    def test_auto_ack_cancel_returns_queued_messages_before_tag_reuse(self):
        old_callback = mock.Mock()
        self._register_consumer('ctag', old_callback, auto_ack=True)
        self.obj._add_pending_event(self._delivery_evt('ctag', 1))
        self.obj._add_pending_event(self._delivery_evt('ctag', 2))

        returned = self.obj.basic_cancel('ctag')
        self._complete_cancel_ok()

        self.assertEqual([m[0].delivery_tag for m in returned], [1, 2])
        self.assertEqual(self.obj._pending_events, deque())
        old_callback.assert_not_called()

        new_callback = mock.Mock()
        self._register_consumer('ctag', new_callback, auto_ack=True)
        self.obj._add_pending_event(self._delivery_evt('ctag', 3))
        self.obj._dispatch_events()

        old_callback.assert_not_called()
        self.assertEqual(
            new_callback.call_args[0][1].delivery_tag, 3,
            'the auto_ack consumer that reused the tag owns only its own '
            'delivery')

    def test_manual_ack_cancel_rejects_each_queued_tag_on_this_channel(self):
        # The nacks must name the impl channel and each queued delivery tag, not
        # merely empty the container.
        callback = mock.Mock()
        self._register_consumer('ctag', callback, auto_ack=False)
        self.obj._add_pending_event(self._delivery_evt('ctag', 10))
        self.obj._add_pending_event(self._delivery_evt('ctag', 11))

        self.assertEqual(self.obj.basic_cancel('ctag'), [])
        self._complete_cancel_ok()

        self.assertEqual(
            self.channel_impl_mock.basic_reject.call_args_list,
            [mock.call(10, requeue=True),
             mock.call(11, requeue=True)])
        self.assertEqual(self.obj._pending_events, deque())
        callback.assert_not_called()

    def test_dispatch_with_a_retired_consumer_drops_its_event(self):
        # A queued delivery whose consumer was retired before dispatch (channel
        # teardown) is dropped instead of raising or reaching a consumer that
        # reuses the tag.
        survivor_callback = mock.Mock()
        self._register_consumer('survivor', survivor_callback)
        self.obj._add_pending_event(self._delivery_evt('survivor', 1))
        self.obj._add_pending_event(self._delivery_evt('gone', 2))
        self.obj._retire_consumer_info('gone')

        self.obj._dispatch_events()

        survivor_callback.assert_called_once()
        self.assertEqual(survivor_callback.call_args[0][1].delivery_tag, 1)

    def test_delivery_to_an_unknown_consumer_while_tearing_down_is_ignored(
            self):
        with mock.patch.object(blocking_connection.LOGGER, 'debug') as debug:
            self.obj._on_consumer_message_delivery(
                self.channel_impl_mock,
                spec.Basic.Deliver(consumer_tag='gone', delivery_tag=1),
                spec.BasicProperties(), b'body')

        self.assertEqual(self.obj._pending_events, deque())
        self.assertTrue(
            any('unknown consumer' in str(c) for c in debug.call_args_list))

    def test_broker_cancel_retirement_differs_from_client_cancel(self):
        # A broker cancel marks the consumer broker-cancelled and queues a
        # cancellation event that retires the entry on dispatch; no client
        # Basic.Cancel frame is sent.
        callback = mock.Mock()
        self._register_consumer('ctag', callback)

        self._broker_cancel('ctag')

        info = self.obj._consumer_infos['ctag']
        self.assertTrue(info.cancelled_by_broker)
        self.channel_impl_mock.basic_cancel.assert_not_called()
        self.assertEqual(len(self.obj._pending_events), 1)
        self.assertIs(type(self.obj._pending_events[0]),
                      blocking_connection._ConsumerCancellationEvt)

        self.obj._dispatch_events()
        self.assertNotIn('ctag', self.obj._consumer_infos)
        callback.assert_not_called()

    def test_broker_cancel_racing_a_client_cancel_keeps_the_client_flow(self):
        # When the broker cancels a consumer the client is already tearing
        # down, the broker event must not overwrite the TEARING_DOWN state that
        # drives the client's own CancelOk completion.
        callback = mock.Mock()
        self._register_consumer('ctag', callback)
        self.obj._consumer_infos['ctag'].state = (
            blocking_connection._ConsumerInfo.TEARING_DOWN)

        self._broker_cancel('ctag')

        self.assertTrue(self.obj._consumer_infos['ctag'].tearing_down)
        self.channel_impl_mock.basic_cancel.assert_not_called()

    def test_broker_cancel_of_an_unknown_consumer_is_ignored(self):
        # The impl layer may deliver a broker cancel after the wrapper already
        # retired the entry during teardown; this must not raise KeyError.
        self._broker_cancel('never-here')
        self.assertEqual(self.obj._pending_events, deque())

    def test_callback_cancelling_its_own_consumer_still_runs_to_completion(
            self):
        delivered = []

        def self_cancelling(chan, method, _properties, _body):
            delivered.append(method.delivery_tag)
            chan.basic_cancel(method.consumer_tag)
            self._complete_cancel_ok()

        self._register_consumer('ctag', self_cancelling)
        self.obj._add_pending_event(self._delivery_evt('ctag', 1))

        self.obj._dispatch_events()

        self.assertEqual(delivered, [1])
        self.assertNotIn('ctag', self.obj._consumer_infos)

    def test_close_drains_pending_events_through_consumer_cancellation(self):
        # close() cancels every consumer first; the manual-ack path rejects each
        # queued delivery on this channel and the auto_ack path returns them, so
        # none are left queued when the impl channel closes.
        auto_ack_callback = mock.Mock()
        self._register_consumer('auto', auto_ack_callback, auto_ack=True)
        manual_ack_callback = mock.Mock()
        self._register_consumer('manual', manual_ack_callback, auto_ack=False)
        self.obj._add_pending_event(self._delivery_evt('auto', 1))
        self.obj._add_pending_event(self._delivery_evt('manual', 2))
        self.channel_impl_mock.basic_cancel.side_effect = (
            lambda **_kwargs: self._complete_cancel_ok())

        self.obj.close()

        self.channel_impl_mock.basic_reject.assert_called_once_with(
            2, requeue=True)
        auto_ack_callback.assert_not_called()
        manual_ack_callback.assert_not_called()
        self.assertEqual(self.obj._pending_events, deque())
        self.channel_impl_mock.close.assert_called_once_with(
            reply_code=0, reply_text='Normal shutdown')
        self.assertEqual(self.obj._consumer_infos, {})

    def test_pending_events_for_two_tags_keep_their_callbacks_apart(self):
        callback_a = mock.Mock()
        callback_b = mock.Mock()
        self._register_consumer('a', callback_a)
        self._register_consumer('b', callback_b)
        self.obj._add_pending_event(self._delivery_evt('a', 1))
        self.obj._add_pending_event(self._delivery_evt('b', 2))
        self.obj._add_pending_event(self._delivery_evt('a', 3))

        self.obj._dispatch_events()

        self.assertEqual(
            [c[0][1].delivery_tag for c in callback_a.call_args_list], [1, 3])
        self.assertEqual(
            [c[0][1].delivery_tag for c in callback_b.call_args_list], [2])
