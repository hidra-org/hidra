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

import os, sys, glob, inspect
from doctest import DocTestCase, DocTestFinder, DocTestParser
from unittest import TestSuite, TextTestRunner, TestLoader


class ListTestLoader(TestLoader):
    suiteClass = list


class TestManager(object):
    tests = None
    loader = None

    def __init__(self):
        self.tests = []
        self.loader = ListTestLoader()

    def get_test_modules(self):
        for name in glob.glob(os.path.join(os.path.dirname(__file__), '*.py')):
            if name == '__init__':
                continue
            module = os.path.basename(name)[:-3]
            yield module

    def load(self):
        for module in self.get_test_modules():
            __import__('tests', {}, {}, [module])

    def find_modules(self, package):
        modules = [package]
        for name in dir(package):
            value = getattr(package, name)
            if (inspect.ismodule(value)) and (
              value.__name__.rpartition('.')[0] == package.__name__):
                modules.extend(self.find_modules(value))
        return modules

    def main(self, test_names = None, print_only = False, coverage = False):
        result = self.run(
          test_names = test_names,
          print_only = print_only,
          coverage = coverage,
        )

        if (not print_only) and (not result.wasSuccessful()):
            sys.exit(1)
        sys.exit(0)

    def run(self, test_names = None, print_only = False, coverage = False):
        if 'inotifyx' in sys.modules:
            raise AssertionError(
              'inotifyx already imported; '
              'this interferes with coverage analysis'
            )

        if coverage:
            import coverage as _coverage

            if int(_coverage.__version__.split('.')[0]) < 3:
                print >>sys.stderr, (
                  'warning: coverage versions < 3 '
                  'are known to produce imperfect results'
                )

            _coverage.use_cache(False)
            _coverage.start()

        try:
            self.load()

            suite = TestSuite()
            runner = TextTestRunner(verbosity = 2)

            for test in self.tests:
                if self.should_run_test(test, test_names):
                    if print_only:
                        print test.id()
                    else:
                        suite.addTest(test)

            if not print_only:
                return runner.run(suite)

        finally:
            if coverage:
                _coverage.stop()
                import inotifyx
                _coverage.report(self.find_modules(inotifyx))

    def should_run_test(self, test, test_names):
        if test_names is None:
            return True

        for test_name in test_names:
            test_name_parts = test_name.split('.')
            relevant_id_parts = test.id().split('.')[:len(test_name_parts)]
            if test_name_parts == relevant_id_parts:
                return True

        return False

    def add_test_suite(self, test_suite):
        self.tests.extend(self.flatten_test_suite(test_suite))

    def flatten_test_suite(self, test_suite):
        tests = []
        if isinstance(test_suite, TestSuite):
            for test in list(test_suite):
                tests.extend(self.flatten_test_suite(test))
        else:
            tests.append(test_suite)
        return tests

    def add_test_case_class(self, test_case_class):
        self.tests.extend(
          self.loader.loadTestsFromTestCase(test_case_class))

    def make_doc_test_case(self, test):
        def __init__(self, *args, **kwargs):
            DocTestCase.__init__(self, test)
        return type(
          '%s_TestCase' % test.name.split('.')[-1],
          (DocTestCase,),
          {'__init__': __init__},
        )

    def get_doc_test_cases_from_string(
      self,
      string,
      name = '<string>',
      filename = '<string>',
      globs = None,
    ):
        if globs is None:
            globs = {}

        # Make sure __name__ == '__main__' checks fail:
        globs = dict(globs)
        globs['__name__'] = None

        parser = DocTestParser()
        test = parser.get_doctest(
          string,
          globs = globs,
          name = name,
          filename = filename,
          lineno = 0,
        )
        test_case = self.make_doc_test_case(test)
        return [test_case]

    def add_doc_test_cases_from_string(self, *args, **kwargs):
        for test_case in self.get_doc_test_cases_from_string(*args, **kwargs):
            self.add_test_case_class(test_case)

    def import_dotted_name(self, name):
        mod = __import__(name)
        components = name.split('.')
        for component in components[1:]:
            try:
                mod = getattr(mod, component)
            except AttributeError:
                raise ImportError('%r has no attribute %s' % (mod, component))
        return mod

    def get_doc_test_cases_from_module(self, name):
        mod = self.import_dotted_name(name)

        finder = DocTestFinder()
        tests = finder.find(mod)

        doc_test_cases = []
        for test in tests:
            doc_test_cases.append(self.make_doc_test_case(test))
        return doc_test_cases

    def add_doc_test_cases_from_module(self, dst_name, src_name = None):
        if src_name is None:
            src_name = dst_name

        for test_case in self.get_doc_test_cases_from_module(src_name):
            test_case.__module__ = dst_name
            self.add_test_case_class(test_case)

    def get_doc_test_cases_from_text_file(self, filename, *args, **kwargs):
        f = open(filename, 'r')
        try:
            data = f.read()
        finally:
            f.close()

        return self.get_doc_test_cases_from_string(data, *args, **kwargs)

    def add_doc_test_cases_from_text_file(self, *args, **kwargs):
        for test_case in self.get_doc_test_cases_from_text_file(
          *args,
          **kwargs
        ):
            self.add_test_case_class(test_case)


manager = TestManager()
