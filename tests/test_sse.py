"""Parsing an Omnigent event stream into SSE frames."""

from __future__ import annotations

import unittest

from sbx_omnigent.sse import parse_sse


class TestParseSse(unittest.TestCase):
    def test_event_and_data(self) -> None:
        frames = list(parse_sse(iter(['event: x', 'data: {"a":1}', ''])))
        self.assertEqual(len(frames), 1)
        self.assertEqual(frames[0].event, 'x')
        self.assertEqual(frames[0].data, '{"a":1}')

    def test_done_sentinel(self) -> None:
        frames = list(parse_sse(iter(['data: [DONE]', ''])))
        self.assertEqual(frames[0].data, '[DONE]')

    def test_comment_and_multiline_data(self) -> None:
        frames = list(
            parse_sse(iter([': keepalive', 'data: a', 'data: b', '']))
        )
        self.assertEqual(frames[0].data, 'a\nb')

    def test_trailing_frame_without_blank(self) -> None:
        frames = list(parse_sse(iter(['event: e', 'data: d'])))
        self.assertEqual(len(frames), 1)


if __name__ == '__main__':
    unittest.main()
