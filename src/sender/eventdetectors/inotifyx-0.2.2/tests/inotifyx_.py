# Copyright (c) 2009-2011 Forest Bond
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

import os, sys, unittest
from unittest import TestCase

import inotifyx

from tests.manager import manager


class TestInotifyx(TestCase):
    fd = None
    workdir = None
    testdir = None

    def setUp(self):
        self.workdir = os.path.abspath(os.getcwd())
        self.testdir = os.path.abspath(
          os.path.join(os.path.dirname(__file__), 'test')
        )
        os.mkdir(self.testdir)
        os.chdir(self.testdir)
        self.fd = inotifyx.init()

    def tearDown(self):
        os.close(self.fd)
        del self.fd
        os.rmdir(self.testdir)
        del self.testdir
        os.chdir(self.workdir)
        del self.workdir

    def _create_file(self, path, content = ''):
        f = open(path, 'w')
        try:
            f.write(content)
        finally:
            f.close()

    def test_file_create(self):
        inotifyx.add_watch(self.fd, self.testdir, inotifyx.IN_CREATE)
        self._create_file('foo')
        try:
            events = inotifyx.get_events(self.fd)
            self.assertEqual(len(events), 1)
            self.assertEqual(events[0].mask, inotifyx.IN_CREATE)
            self.assertEqual(events[0].name, 'foo')
        finally:
            os.unlink('foo')

    def test_file_remove(self):
        self._create_file('foo')
        try:
            inotifyx.add_watch(self.fd, self.testdir, inotifyx.IN_DELETE)
        except:
            os.unlink('foo')
            raise
        os.unlink('foo')
        events = inotifyx.get_events(self.fd)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].mask, inotifyx.IN_DELETE)
        self.assertEqual(events[0].name, 'foo')

    def test_version_attribute(self):
        from inotifyx import distinfo
        self.assertEqual(inotifyx.__version__, distinfo.version)


manager.add_test_case_class(TestInotifyx)
