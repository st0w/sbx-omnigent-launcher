"""The pipeline image builder (#38): built once per recipe."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
import unittest
from collections.abc import Sequence

from sbx_omnigent import image, sbx_cli
from sbx_omnigent.pipeline import ImageSpec

_TAG = 'localhost/omni-sbx-demo:0123456789abcdef'
_BOX = 'image-build-demo-abc123'
_SPEC = ImageSpec(
    base='ghcr.io/omnigent-ai/omnigent-host:v0.20.0',
    setup='apt-get update\napt-get install -y build-essential',
)


def _listing(*refs: str) -> str:
    images = []
    for ref in refs:
        repository, _, tag = ref.rpartition(':')
        images.append({'id': 'x', 'repository': repository, 'tag': tag})
    return json.dumps({'images': images})


class _FakeSbx:
    """Records every ``sbx`` argv; answers each verb as configured."""

    def __init__(
        self,
        *,
        listing: str = _listing(),
        fail: str | None = None,
        hang: str | None = None,
        output: str = 'setup ran',
    ) -> None:
        self.calls: list[list[str]] = []
        self.timeouts: list[float] = []
        self._listing = listing
        self._fail = fail
        self._hang = hang
        self._output = output

    def __call__(
        self, argv: Sequence[str], **kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        command = list(argv)
        self.calls.append(command)
        timeout = kwargs.get('timeout')
        assert isinstance(timeout, float)
        self.timeouts.append(timeout)
        verb = command[2] if command[1] in ('template', 'policy') else (
            command[1]
        )
        if verb == self._hang:
            raise subprocess.TimeoutExpired(command, timeout)
        rc = 1 if verb == self._fail else 0
        stdout = ''
        if verb == 'ls':
            stdout = self._listing
        elif verb == 'exec':
            stdout = self._output
        return subprocess.CompletedProcess(command, rc, stdout, 'boom')


def _build(sbx: _FakeSbx, egress: tuple[str, ...] = ('deb.debian.org',),
           spec: ImageSpec = _SPEC) -> None:
    image.build_image(
        tag=_TAG, spec=spec, name=_BOX, egress=egress, runner=sbx
    )


class TestWhetherTheImageIsBuilt(unittest.TestCase):
    def test_a_listed_tag_is_built(self) -> None:
        sbx = _FakeSbx(listing=_listing('ghcr.io/x/host:v1', _TAG))
        self.assertTrue(image.image_built(_TAG, runner=sbx))
        self.assertEqual(sbx.calls, [['sbx', 'template', 'ls', '--json']])

    def test_an_unlisted_tag_is_not(self) -> None:
        sbx = _FakeSbx(listing=_listing(
            'localhost/omni-sbx-demo:ffffffffffffffff'
        ))
        self.assertFalse(image.image_built(_TAG, runner=sbx))

    def test_an_empty_store_has_nothing(self) -> None:
        sbx = _FakeSbx(listing='{"images": null}')
        self.assertFalse(image.image_built(_TAG, runner=sbx))

    def test_a_failed_listing_raises(self) -> None:
        with self.assertRaises(image.ImageError) as caught:
            image.image_built(_TAG, runner=_FakeSbx(fail='ls'))
        self.assertIn('could not list', str(caught.exception))

    def test_unreadable_output_raises(self) -> None:
        for listing in ('not json', '[]', '{"images": [1]}'):
            with self.subTest(listing=listing):
                with self.assertRaises(image.ImageError):
                    image.image_built(
                        _TAG, runner=_FakeSbx(listing=listing)
                    )


class TestBuildingTheImage(unittest.TestCase):
    def test_create_scope_setup_stop_save_remove(self) -> None:
        sbx = _FakeSbx()
        _build(sbx, egress=('deb.debian.org', 'static.rust-lang.org'))
        self.assertEqual(sbx.calls, [
            ['sbx', 'create', 'shell', '--template', _SPEC.base,
             '--name', _BOX, '--quiet'],
            ['sbx', 'policy', 'allow', 'network', '--sandbox', _BOX,
             'deb.debian.org,static.rust-lang.org'],
            ['sbx', 'exec', _BOX, '--', 'sh', '-c',
             'set -e\napt-get update\napt-get install -y build-essential'],
            ['sbx', 'stop', _BOX],
            ['sbx', 'template', 'save', _BOX, _TAG],
            ['sbx', 'rm', '--force', _BOX],
        ])

    def test_the_build_box_mounts_nothing(self) -> None:
        sbx = _FakeSbx()
        _build(sbx)
        create = sbx.calls[0]
        self.assertEqual(create[3], '--template')

    def test_no_allowlist_adds_no_rule(self) -> None:
        sbx = _FakeSbx()
        _build(sbx, egress=())
        self.assertNotIn('policy', [c[1] for c in sbx.calls])

    def test_resources_reach_the_build_box(self) -> None:
        sbx = _FakeSbx()
        _build(sbx, spec=ImageSpec(
            base=_SPEC.base, setup='true', cpus=4, memory='8g'
        ))
        self.assertEqual(sbx.calls[0][-4:], ['--cpus', '4', '--memory', '8g'])

    def test_the_setup_gets_its_own_timeout(self) -> None:
        sbx = _FakeSbx()
        _build(sbx, spec=ImageSpec(
            base=_SPEC.base, setup='true', timeout_s=2400.0
        ))
        exec_at = [c[1] for c in sbx.calls].index('exec')
        self.assertEqual(sbx.timeouts[exec_at], 2400.0)

    def test_a_failed_setup_saves_nothing_and_says_why(self) -> None:
        sbx = _FakeSbx(fail='exec', output='E: Unable to locate package x')
        with self.assertRaises(image.ImageError) as caught:
            _build(sbx)
        message = str(caught.exception)
        self.assertIn('image.setup failed (exit 1)', message)
        self.assertIn('E: Unable to locate package x', message)
        self.assertNotIn('save', [c[2] for c in sbx.calls if len(c) > 2])
        self.assertEqual(sbx.calls[-1], ['sbx', 'rm', '--force', _BOX])

    def test_a_setup_past_its_timeout_says_so(self) -> None:
        sbx = _FakeSbx(hang='exec')
        with self.assertRaises(image.ImageError) as caught:
            _build(sbx)
        self.assertIn('did not finish within 3600s', str(caught.exception))
        self.assertEqual(sbx.calls[-1], ['sbx', 'rm', '--force', _BOX])

    def test_every_failure_removes_the_box(self) -> None:
        for verb in ('policy', 'stop', 'save'):
            with self.subTest(verb=verb):
                fail = 'allow' if verb == 'policy' else verb
                sbx = _FakeSbx(fail=fail)
                with self.assertRaises(image.ImageError):
                    _build(sbx)
                self.assertEqual(
                    sbx.calls[-1], ['sbx', 'rm', '--force', _BOX]
                )

    def test_a_failed_create_still_removes_any_partial_box(self) -> None:
        sbx = _FakeSbx(fail='create')
        with self.assertRaises(image.ImageError) as caught:
            _build(sbx)
        self.assertIn('could not create', str(caught.exception))
        self.assertEqual(len(sbx.calls), 2)
        self.assertEqual(sbx.calls[-1], ['sbx', 'rm', '--force', _BOX])

    def test_a_stuck_daemon_is_named(self) -> None:
        sbx = _FakeSbx(hang='create')
        with self.assertRaises(image.ImageError) as caught:
            _build(sbx)
        self.assertIn('sbx create', str(caught.exception))

    def test_setup_output_is_scrubbed(self) -> None:
        sbx = _FakeSbx(
            fail='exec', output='login failed: password=s3cr3t-value'
        )
        with self.assertRaises(image.ImageError) as caught:
            _build(sbx)
        self.assertNotIn('s3cr3t-value', str(caught.exception))


class TestTheBuildBoxName(unittest.TestCase):
    def test_it_names_the_pipeline_and_is_unique(self) -> None:
        first = image.build_box_name('demo')
        self.assertRegex(first, r'^image-build-demo-[0-9a-f]{6}$')
        self.assertNotEqual(first, image.build_box_name('demo'))

    def test_a_long_name_fits(self) -> None:
        self.assertLessEqual(len(image.build_box_name('p' * 200)), 56)


class TestTheTimeoutTiers(unittest.TestCase):
    def test_saving_gets_longer_than_a_management_call(self) -> None:
        # 3 GB took 145 s to save in a spike; a management budget would
        # call a healthy save a stuck daemon.
        self.assertGreater(image.SAVE_TIMEOUT_S, sbx_cli.MANAGE_TIMEOUT_S)


class TestTheServerPublishesItsAgentImage(unittest.TestCase):
    """The runner builds the image; the server's agents boot it. Each
    must know the other means the same one (#38)."""

    def setUp(self) -> None:
        self.root = tempfile.mkdtemp(prefix='img-pub-')
        self.addCleanup(shutil.rmtree, self.root, True)

    def test_what_is_published_is_read_back(self) -> None:
        image.publish_agent_image(self.root, _TAG)
        self.assertEqual(image.read_agent_image(self.root), _TAG)

    def test_it_creates_the_directory(self) -> None:
        root = os.path.join(self.root, 'new')
        image.publish_agent_image(root, _TAG)
        self.assertEqual(image.read_agent_image(root), _TAG)

    def test_it_is_readable_by_the_owner_only(self) -> None:
        image.publish_agent_image(self.root, _TAG)
        mode = os.stat(os.path.join(self.root, image.AGENT_IMAGE_FILE))
        self.assertEqual(mode.st_mode & 0o077, 0)

    def test_a_missing_file_says_restart_the_server(self) -> None:
        with self.assertRaises(image.ImageError) as caught:
            image.read_agent_image(self.root)
        self.assertIn('Restart the server', str(caught.exception))

    def test_a_malformed_file_is_refused(self) -> None:
        path = os.path.join(self.root, image.AGENT_IMAGE_FILE)
        for text in ('nope', '[]', '{"version": 1}',
                     '{"version": 1, "agent_image": ""}',
                     '{"version": 2, "agent_image": "x:1"}'):
            with self.subTest(text=text):
                with open(path, 'w', encoding='utf-8') as fh:
                    fh.write(text)
                with self.assertRaises(image.ImageError):
                    image.read_agent_image(self.root)


if __name__ == '__main__':
    unittest.main()
