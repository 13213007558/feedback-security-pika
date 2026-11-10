"""Tests for pika.channel.ContentFrameAssembler."""

import marshal
import unittest

from pika import channel, exceptions, frame, spec


def _method(channel_number=1, delivery_tag=1):
    return frame.Method(
        channel_number,
        spec.Basic.Deliver(consumer_tag='ctag0', delivery_tag=delivery_tag))


def _header(channel_number=1, body_size=6, correlation_id=None):
    properties = spec.BasicProperties(
        correlation_id=correlation_id) if correlation_id else spec.BasicProperties()
    return frame.Header(channel_number, body_size, properties)


def _body(channel_number=1, fragment=b'abc123'):
    return frame.Body(channel_number, fragment)


class ContentFrameAssemblerTests(unittest.TestCase):

    def setUp(self):
        self.obj = channel.ContentFrameAssembler()

    def assert_reset(self):
        self.assertEqual(self.obj._state,
                         channel.ContentFrameAssembler.AWAIT_METHOD)
        self.assertIsNone(self.obj._method_frame)
        self.assertIsNone(self.obj._header_frame)
        self.assertEqual(self.obj._seen_so_far, 0)
        self.assertEqual(self.obj._body_fragments, [])

    def test_init_state_awaits_method(self):
        self.assertEqual(self.obj._state,
                         channel.ContentFrameAssembler.AWAIT_METHOD)

    def test_init_method_frame(self):
        self.assertEqual(self.obj._method_frame, None)

    def test_init_header_frame(self):
        self.assertEqual(self.obj._header_frame, None)

    def test_init_seen_so_far(self):
        self.assertEqual(self.obj._seen_so_far, 0)

    def test_init_body_fragments(self):
        self.assertEqual(self.obj._body_fragments, [])

    def test_process_with_full_message(self):
        method_frame = _method()
        header_frame = _header()
        body_frame = _body()
        self.assertIsNone(self.obj.process(method_frame))
        self.assertIsNone(self.obj.process(header_frame))
        response = self.obj.process(body_frame)
        self.assertEqual(response, (method_frame, header_frame, b'abc123'))
        self.assert_reset()

    def test_process_with_body_split_across_frames(self):
        method_frame = _method()
        header_frame = _header(body_size=6)
        self.obj.process(method_frame)
        self.obj.process(header_frame)
        self.assertIsNone(self.obj.process(_body(fragment=b'abc')))
        self.assertEqual(self.obj._seen_so_far, 3)
        response = self.obj.process(_body(fragment=b'123'))
        self.assertEqual(response, (method_frame, header_frame, b'abc123'))
        self.assert_reset()

    def test_process_with_zero_length_body_delivers_immediately(self):
        method_frame = _method(delivery_tag=7)
        header_frame = _header(body_size=0, correlation_id='empty')
        self.obj.process(method_frame)
        response = self.obj.process(header_frame)
        self.assertEqual(response, (method_frame, header_frame, b''))
        self.assert_reset()

    def test_process_with_two_consecutive_empty_messages(self):
        first_method = _method(delivery_tag=1)
        first_header = _header(body_size=0, correlation_id='first')
        second_method = _method(delivery_tag=2)
        second_header = _header(body_size=0, correlation_id='second')

        self.obj.process(first_method)
        first = self.obj.process(first_header)
        self.obj.process(second_method)
        second = self.obj.process(second_header)

        self.assertEqual(first, (first_method, first_header, b''))
        self.assertEqual(second, (second_method, second_header, b''))
        self.assert_reset()

    def test_process_with_two_consecutive_nonempty_messages_keeps_them_apart(
            self):
        first_method = _method(delivery_tag=1)
        first_header = _header(body_size=3, correlation_id='first')
        second_method = _method(delivery_tag=2)
        second_header = _header(body_size=4, correlation_id='second')

        self.obj.process(first_method)
        self.obj.process(first_header)
        first = self.obj.process(_body(fragment=b'one'))
        self.obj.process(second_method)
        self.obj.process(second_header)
        second = self.obj.process(_body(fragment=b'four'))

        self.assertEqual(first, (first_method, first_header, b'one'))
        self.assertEqual(second, (second_method, second_header, b'four'))

    def test_process_with_body_frame_partial_tracks_fragment(self):
        self.obj.process(_method())
        self.obj.process(_header(body_size=10))
        body_frame = _body(fragment=b'abc123')
        self.obj.process(body_frame)
        self.assertEqual(self.obj._body_fragments, [body_frame.fragment])
        self.assertEqual(self.obj._seen_so_far, 6)

    def test_process_with_body_frame_six_bytes(self):
        self.obj.process(_method())
        self.obj.process(_header(body_size=10))
        self.obj.process(_body(fragment=b'abc123'))
        self.assertEqual(self.obj._seen_so_far, 6)

    def test_process_with_body_frame_too_big(self):
        self.obj.process(_method())
        self.obj.process(_header(body_size=6))
        with self.assertRaises(exceptions.BodyTooLongError) as ctx:
            self.obj.process(_body(fragment=b'abcd1234'))
        self.assertEqual(ctx.exception.args, (8, 6))
        self.assert_reset()

    def test_process_with_body_frame_too_big_after_partial_fragments(self):
        self.obj.process(_method())
        self.obj.process(_header(body_size=6))
        self.obj.process(_body(fragment=b'abc'))
        with self.assertRaises(exceptions.BodyTooLongError) as ctx:
            self.obj.process(_body(fragment=b'12345'))
        self.assertEqual(ctx.exception.args, (8, 6))
        self.assert_reset()

    def test_process_with_unexpected_frame_type(self):
        value = frame.Method(1, spec.Basic.Qos())
        self.assertRaises(exceptions.UnexpectedFrameError, self.obj.process,
                          value)

    def test_process_with_non_content_method_frame(self):
        value = frame.Method(1, spec.Basic.Ack(delivery_tag=1))
        self.assertRaises(exceptions.UnexpectedFrameError, self.obj.process,
                          value)
        self.assert_reset()

    def test_header_before_method_is_rejected(self):
        with self.assertRaises(exceptions.UnexpectedFrameError):
            self.obj.process(_header())
        self.assert_reset()

    def test_duplicate_header_is_rejected(self):
        self.obj.process(_method())
        header_frame = _header()
        self.obj.process(header_frame)
        with self.assertRaises(exceptions.UnexpectedFrameError):
            self.obj.process(_header(body_size=9))
        self.assert_reset()

    def test_body_before_header_is_rejected(self):
        self.obj.process(_method())
        with self.assertRaises(exceptions.UnexpectedFrameError):
            self.obj.process(_body())
        self.assert_reset()

    def test_body_without_any_method_is_rejected(self):
        with self.assertRaises(exceptions.UnexpectedFrameError):
            self.obj.process(_body())
        self.assert_reset()

    def test_method_arriving_mid_body_does_not_overwrite_assembly(self):
        # Minimal pre-fix mismatch sequence: Deliver, Header, partial Body, then
        # a second Deliver. The old assembler overwrote the pending method, so
        # the remaining body finished a message carrying the second method with
        # the first message's bytes. The state machine rejects the second method
        # and releases the first message's fragments instead.
        first_method = _method(delivery_tag=1)
        self.obj.process(first_method)
        self.obj.process(_header(body_size=6))
        self.obj.process(_body(fragment=b'abc'))
        second_method = _method(delivery_tag=2)
        with self.assertRaises(exceptions.UnexpectedFrameError):
            self.obj.process(second_method)
        self.assert_reset()
        # The rejected message's method never replaces the one being assembled.
        self.assertIsNone(self.obj._method_frame)

    def test_zero_length_body_frame_is_rejected(self):
        self.obj.process(_method())
        self.obj.process(_header(body_size=3))
        with self.assertRaises(exceptions.UnexpectedFrameError):
            self.obj.process(_body(fragment=b''))
        self.assert_reset()

    def test_rejected_frame_releases_buffered_fragments(self):
        method_frame = _method()
        self.obj.process(method_frame)
        self.obj.process(_header(body_size=10))
        self.obj.process(_body(fragment=b'abc'))
        with self.assertRaises(exceptions.UnexpectedFrameError):
            self.obj.process(_header(body_size=10))
        self.assert_reset()

    def test_assembler_recovers_after_protocol_error(self):
        with self.assertRaises(exceptions.UnexpectedFrameError):
            self.obj.process(_body())
        method_frame = _method(delivery_tag=9)
        header_frame = _header(body_size=2, correlation_id='recovered')
        body_frame = _body(fragment=b'ok')
        self.obj.process(method_frame)
        self.obj.process(header_frame)
        response = self.obj.process(body_frame)
        self.assertEqual(response, (method_frame, header_frame, b'ok'))

    def test_reset_clears_every_field(self):
        self.obj.process(_method())
        self.obj.process(_header(body_size=10))
        self.obj.process(_body(fragment=b'abc123'))
        self.obj._reset()
        self.assert_reset()

    def test_ascii_bytes_body_instance(self):
        self.obj.process(_method())
        header_frame = _header(body_size=11)
        self.obj.process(header_frame)
        result = self.obj.process(_body(fragment=b'foo-bar-baz'))
        assert result is not None
        method_frame, header_frame, body_value = result
        self.assertIsInstance(body_value, bytes)

    def test_ascii_body_value(self):
        expectation = b'foo-bar-baz'
        self.obj = channel.ContentFrameAssembler()
        self.obj.process(_method())
        self.obj.process(_header(body_size=11))
        result = self.obj.process(_body(fragment=b'foo-bar-baz'))
        assert result is not None
        method_frame, header_frame, body_value = result
        self.assertEqual(body_value, expectation)
        self.assertIsInstance(body_value, bytes)

    def test_binary_non_unicode_value(self):
        expectation = ('a', 0.8)
        self.obj = channel.ContentFrameAssembler()
        marshalled_body = marshal.dumps(expectation)
        method_frame = _method()
        header_frame = _header(body_size=len(marshalled_body))
        self.obj.process(method_frame)
        self.obj.process(header_frame)
        body_frame = _body(fragment=marshalled_body)
        result = self.obj.process(body_frame)
        assert result is not None
        method_frame, header_frame, body_value = result
        self.assertEqual(marshal.loads(body_value), expectation)
