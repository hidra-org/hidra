# Copyright (c) 2011 Forest Bond
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

import glob, os


def get_test_modules():
    for name in glob.glob(os.path.join(os.path.dirname(__file__), '*.py')):
        if name == '__init__':
            continue
        module = os.path.basename(name)[:-3]
        yield module


def load():
    for module in get_test_modules():
        __import__('tests', {}, {}, [module])


def run(**kwargs):
    from tests.manager import manager
    manager.run(**kwargs)


def main(**kwargs):
    from tests.manager import manager
    manager.main(**kwargs)


def print_names(test_names = None):
    from tests.manager import manager
    print '\n'.join(manager.get_names(test_names))
