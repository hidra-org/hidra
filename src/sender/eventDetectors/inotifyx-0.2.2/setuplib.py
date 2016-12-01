# Author: Forest Bond
# This file is in the public domain.

from __future__ import with_statement

import os, sys, shutil
from tempfile import mkdtemp
from modulefinder import Module
from distutils.command.build import build
from distutils.command.clean import clean as _clean
from distutils.command.install import install
from distutils.command.install_lib import install_lib as _install_lib
from distutils.command.install_data import install_data as _install_data
from distutils.core import (
  setup as _setup,
  Command,
)
from distutils.spawn import spawn
from distutils import log
from distutils.dir_util import remove_tree
from distutils.dist import Distribution

try:
    from py2exe.build_exe import py2exe as _py2exe
except ImportError:
    _py2exe = None
else:
    from py2exe.build_exe import byte_compile


###


class test(Command):
    description = 'run tests'
    user_options = [
      ('tests=', None, 'names of tests to run'),
      ('print-only', None, "don't run tests, just print their names"),
      ('coverage', None, "print coverage analysis (requires coverage.py)"),
    ]

    def initialize_options(self):
        self.tests = None
        self.print_only = False
        self.coverage = False

    def finalize_options(self):
        if self.tests is not None:
            self.tests = self.tests.split(',')

    def run(self):
        build_obj = self.get_finalized_command('build')
        build_obj.run()
        sys.path.insert(0, build_obj.build_lib)
        try:
            mod = __import__(self.distribution.test_module)
            main = getattr(mod, 'main')
            main(
              test_names = self.tests,
              print_only = self.print_only,
              coverage = self.coverage,
            )
        finally:
            sys.path.remove(build_obj.build_lib)


Distribution.test_module = 'tests'


###


class clean(_clean):
    user_options = _clean.user_options + [
      ('build-man=', None, 'manpage build directory'),
    ]

    def initialize_options(self):
        _clean.initialize_options(self)
        self.build_man = None

    def finalize_options(self):
        _clean.finalize_options(self)
        self.set_undefined_options('build_man', ('build_dir', 'build_man'))

    def run(self):
        if self.all:
            if os.path.exists(self.build_man):
                remove_tree(self.build_man, dry_run = self.dry_run)
            else:
                log.debug(
                  "'%s' does not exist -- can't clean it",
                  self.build_man,
                )

        _clean.run(self)


###


class build_man(Command):
    user_options = _install_data.user_options + [
      ('build-dir=', 'b', 'manpage build directory'),
    ]
    description = 'Build manual pages from docbook XML.'

    xsltproc = ['xsltproc', '--nonet', '--novalid', '--xinclude']

    def initialize_options(self):
        self.build_base = None
        self.build_dir = None

    def finalize_options(self):
        self.set_undefined_options('build', ('build_base', 'build_base'))
        if self.build_dir is None:
            self.build_dir = os.path.join(self.build_base, 'man')
        self.docbook_files = []
        if self.distribution.manpage_sources is not None:
            unclaimed_files = list(self.distribution.manpage_sources)
            for index, filename in enumerate(list(unclaimed_files)):
                if filename.endswith('.xml'):
                    self.docbook_files.append(os.path.abspath(filename))
                    del unclaimed_files[index]
            if unclaimed_files:
                log.error(
                  'unknown manpage source file types: %s',
                  ', '.join(unclaimed_files),
                )
                raise SystemExit(1)

    def _find_docbook_manpage_stylesheet(self):
        from libxml2 import catalogResolveURI
        return catalogResolveURI(
          'http://docbook.sourceforge.net/release/xsl/current/manpages/docbook.xsl'
        )

    def build_manpage_from_docbook(self, stylesheet, docbook_file):
        if stylesheet is None:
            raise AssertionError('stylesheet is None')

        command = self.xsltproc + [stylesheet, docbook_file]
        orig_wd = os.getcwd()
        os.chdir(self.build_dir)
        try:
            spawn(command, dry_run = self.dry_run)
        finally:
            os.chdir(orig_wd)

    def run(self):
        if self.docbook_files:
            stylesheet = self._find_docbook_manpage_stylesheet()

            if stylesheet is None:
                log.warn(
                  'Warning: missing docbook XSL stylesheets; '
                  'manpages will not be built.\n'
                  'Please install the docbook XSL stylesheets from '
                  'http://docbook.org/.'
                )

            else:
                if not self.dry_run:
                    if not os.path.exists(self.build_dir):
                        os.mkdir(self.build_dir)
                for docbook_file in self.docbook_files:
                    log.info('building manpage from docbook: %s', docbook_file)
                    if not self.dry_run:
                        self.build_manpage_from_docbook(
                          stylesheet,
                          docbook_file,
                        )

Distribution.manpage_sources = None

build.sub_commands.append((
  'build_man',
  (lambda self: bool(self.distribution.manpage_sources)),
))


class install_man(_install_data):
    def initialize_options(self):
        _install_data.initialize_options(self)
        self.build_dir = None

    def finalize_options(self):
        _install_data.finalize_options(self)
        self.set_undefined_options('build_man', ('build_dir', 'build_dir'))

        self.data_files = []

        if os.path.exists(self.build_dir):
            for entry in os.listdir(self.build_dir):
                path = os.path.join(self.build_dir, entry)
                if os.path.isfile(path):
                    base, ext = os.path.splitext(entry)
                    section = int(ext[1:])
                    self.data_files.append(
                      ('share/man/man%u' % section, [path])
                    )

install.sub_commands.append((
  'install_man',
  (lambda self: bool(self.distribution.manpage_sources)),
))


###


class _DistinfoMixin:
    def _split_distinfo_module(self):
        parts = self.distribution.distinfo_module.split('.')
        return parts[:-1], parts[-1]

    def _prepare_distinfo_string(self, value):
        if isinstance(value, str):
            value = unicode(value)
        return unicode(repr(value)).encode('utf-8')

    def _write_distinfo_module(self, outfile, distinfo = (), imports = ()):
        distinfo = list(distinfo)
        imports = list(imports)

        distinfo.insert(
          0,
          (
            'version',
            self._prepare_distinfo_string(self.distribution.metadata.version),
          ),
        )

        log.info("creating distinfo file %r:", outfile)
        for k, v in distinfo:
            log.info(' %s = %s', k, v)

        if not self.dry_run:
            with open(outfile, 'wb') as f:
                f.write('# coding: utf-8\n')
                f.write('\n')
                for modname in imports:
                    f.write('import %s\n' % modname)
                if imports:
                    f.write('\n')
                for k, v in distinfo:
                    f.write('%s = %s\n' % (k, v))


###


class install_data(_install_data):
    user_options = _install_data.user_options + [
      (
        'install-dir-arch=',
        None,
        'base directory for installing architecture-dependent data files',
      ),
      (
        'install-dir-arch-pkg=',
        None,
        'package-specific directory for installing architecture-dependent data files',
      ),
      (
        'install-dir-indep=',
        None,
        'base directory for installing architecture-independent data files',
      ),
      (
        'install-dir-indep-pkg=',
        None,
        'package-specific directory for installing architecture-independent data files',
      ),
    ]

    def initialize_options(self):
        _install_data.initialize_options(self)

        self.install_dir_arch = None
        self.install_dir_arch_pkg = None
        self.install_dir_indep = None
        self.install_dir_indep_pkg = None

        self.data_files_arch = self.distribution.data_files_arch
        self.data_files_arch_pkg = self.distribution.data_files_arch_pkg
        self.data_files_indep = self.distribution.data_files_indep
        self.data_files_indep_pkg = self.distribution.data_files_indep_pkg

    def _get_relative_install_dir(self, p):
        return p[len(self.install_dir) + len(os.sep):]

    def finalize_options(self):
        _install_data.finalize_options(self)

        py2exe_obj = self.distribution.get_command_obj('py2exe', False)
        if py2exe_obj is not None:
            py2exe_obj.ensure_finalized()

        if (py2exe_obj is not None) and (
          self.install_dir == py2exe_obj.dist_dir
        ):
            self.install_dir_arch = self.install_dir
            self.install_dir_arch_pkg = self.install_dir
            self.install_dir_indep = self.install_dir
            self.install_dir_indep_pkg = self.install_dir

        else:
            if self.install_dir_arch is None:
                if sys.platform == 'win32':
                    self.install_dir_arch = self.install_dir
                else:
                    self.install_dir_arch = os.path.join(self.install_dir, 'lib')

            if self.install_dir_arch_pkg is None:
                self.install_dir_arch_pkg = os.path.join(
                  self.install_dir_arch,
                  self.distribution.metadata.name,
                )

            if self.install_dir_indep is None:
                if sys.platform == 'win32':
                    self.install_dir_indep = self.install_dir
                else:
                    self.install_dir_indep = os.path.join(self.install_dir, 'share')

            if self.install_dir_indep_pkg is None:
                self.install_dir_indep_pkg = os.path.join(
                  self.install_dir_indep,
                  self.distribution.metadata.name,
                )

        if self.data_files is None:
            self.data_files = []

        if self.data_files_arch:
            self.data_files.extend(self._gen_data_files(
              self._get_relative_install_dir(self.install_dir_arch),
              self.data_files_arch,
            ))

        if self.data_files_arch_pkg:
            self.data_files.extend(self._gen_data_files(
              self._get_relative_install_dir(self.install_dir_arch_pkg),
              self.data_files_arch_pkg,
            ))

        if self.data_files_indep:
            self.data_files.extend(self._gen_data_files(
              self._get_relative_install_dir(self.install_dir_indep),
              self.data_files_indep,
            ))

        if self.data_files_indep_pkg:
            self.data_files.extend(self._gen_data_files(
              self._get_relative_install_dir(self.install_dir_indep_pkg),
              self.data_files_indep_pkg,
            ))

    def _gen_data_files(self, base_dir, data_files):
        for arg in data_files:
            print arg
            if isinstance(arg, basestring):
                yield (base_dir, [arg])
            else:
                subdir, filenames = arg
                yield (os.path.join(base_dir, subdir), filenames)


Distribution.data_files_arch = None
Distribution.data_files_arch_pkg = None
Distribution.data_files_indep = None
Distribution.data_files_indep_pkg = None


orig_has_data_files = Distribution.has_data_files

def has_data_files(self):
    if orig_has_data_files(self):
        return True
    return any([
      self.data_files_arch,
      self.data_files_arch_pkg,
      self.data_files_indep,
      self.data_files_indep_pkg,
    ])

Distribution.has_data_files = has_data_files


###


class install_lib(_DistinfoMixin, _install_lib):
    def initialize_options(self):
        _install_lib.initialize_options(self)

        self.distinfo_package = None
        self.distinfo_module = None

    def finalize_options(self):
        _install_lib.finalize_options(self)

        if self.distribution.distinfo_module is not None:
            self.distinfo_package, self.distinfo_module = \
              self._split_distinfo_module()

    def install(self):
        retval = _install_lib.install(self)

        if retval is None:
            return retval

        py2exe_obj = self.distribution.get_command_obj('py2exe', False)

        if (py2exe_obj is None) and (self.distinfo_package is not None):
            parts = [self.install_dir]
            parts.extend(self.distinfo_package)
            parts.append('%s.py' % self.distinfo_module)
            installed_module_path = os.path.join(*parts)

            install_obj = self.get_finalized_command('install')
            install_data_obj = self.get_finalized_command('install_data')

            distinfo = [
              (
                'install_base',
                self._prepare_distinfo_string(os.path.abspath(
                  install_obj.install_base,
                )),
              ),
              (
                'install_platbase',
                self._prepare_distinfo_string(os.path.abspath(
                  install_obj.install_platbase,
                )),
              ),
              (
                'install_purelib',
                self._prepare_distinfo_string(os.path.abspath(
                  install_obj.install_purelib,
                )),
              ),
              (
                'install_platlib',
                self._prepare_distinfo_string(os.path.abspath(
                  install_obj.install_platlib,
                )),
              ),
              (
                'install_lib',
                self._prepare_distinfo_string(os.path.abspath(
                  install_obj.install_lib,
                )),
              ),
              (
                'install_headers',
                self._prepare_distinfo_string(os.path.abspath(
                  install_obj.install_headers,
                )),
              ),
              (
                'install_scripts',
                self._prepare_distinfo_string(os.path.abspath(
                  install_obj.install_scripts,
                )),
              ),
              (
                'install_data',
                self._prepare_distinfo_string(os.path.abspath(
                  install_data_obj.install_dir,
                )),
              ),
              (
                'install_data_arch',
                self._prepare_distinfo_string(os.path.abspath(
                  install_data_obj.install_dir_arch,
                )),
              ),
              (
                'install_data_arch_pkg',
                self._prepare_distinfo_string(os.path.abspath(
                  install_data_obj.install_dir_arch_pkg,
                )),
              ),
              (
                'install_data_indep',
                self._prepare_distinfo_string(os.path.abspath(
                  install_data_obj.install_dir_indep,
                )),
              ),
              (
                'install_data_indep_pkg',
                self._prepare_distinfo_string(os.path.abspath(
                  install_data_obj.install_dir_indep_pkg,
                )),
              ),
            ]

            self._write_distinfo_module(installed_module_path, distinfo)

            retval.append(installed_module_path)

        return retval


if _py2exe is None:
    py2exe = None
else:
    class py2exe(_DistinfoMixin, _py2exe):
        def make_lib_archive(self, *args, **kwargs):
            if self.distribution.distinfo_module is not None:
                imports = ['os', 'sys']
                distinfo = [
                  (
                    'install_data',
                    'os.path.dirname(sys.executable)',
                  ),
                  (
                    'install_data_arch',
                    'os.path.dirname(sys.executable)',
                  ),
                  (
                    'install_data_arch_pkg',
                    'os.path.dirname(sys.executable)',
                  ),
                  (
                    'install_data_indep',
                    'os.path.dirname(sys.executable)',
                  ),
                  (
                    'install_data_indep_pkg',
                    'os.path.dirname(sys.executable)',
                  ),
                ]

                tmp_dir_path = mkdtemp()
                try:
                    distinfo_package, distinfo_module = self._split_distinfo_module()
                    tmp_file_parent_path = os.path.join(
                      tmp_dir_path,
                      *distinfo_package
                    )
                    os.makedirs(tmp_file_parent_path)
                    tmp_file_path = os.path.join(
                      tmp_file_parent_path,
                      ('%s.py' % distinfo_module),
                    )
                    self._write_distinfo_module(tmp_file_path, distinfo, imports)
                    sys.path.insert(0, tmp_dir_path)
                    try:
                        self._distinfo_compiled_files = byte_compile(
                          [Module(
                            name = self.distribution.distinfo_module,
                            file = tmp_file_path,
                          )],
                          target_dir = self.collect_dir,
                          optimize = self.optimize,
                          force = 0,
                          verbose = self.verbose,
                          dry_run = self.dry_run,
                        )
                    finally:
                        del sys.path[0]
                finally:
                    shutil.rmtree(tmp_dir_path)

            self.compiled_files.extend(self._distinfo_compiled_files)

            return _py2exe.make_lib_archive(self, *args, **kwargs)


Distribution.distinfo_module = None


###


def setup(*args, **kwargs):
    cmdclass = kwargs.setdefault('cmdclass', {})
    kwargs['cmdclass'].setdefault('test', test)
    kwargs['cmdclass'].setdefault('install_data', install_data)
    kwargs['cmdclass'].setdefault('install_lib', install_lib)
    kwargs['cmdclass'].setdefault('build_man', build_man)
    kwargs['cmdclass'].setdefault('install_man', install_man)
    kwargs['cmdclass'].setdefault('clean', clean)
    if py2exe is not None:
        kwargs['cmdclass'].setdefault('py2exe', py2exe)
    return _setup(*args, **kwargs)
