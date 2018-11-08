# Copyright (C) 2015  DESY, Manuela Kuhn, Notkestr. 85, D-22607 Hamburg
#
# HiDRA is a generic tool set for high performance data multiplexing with
# different qualities of service and based on Python and ZeroMQ.
#
# This software is free: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 2 of the License, or
# (at your option) any later version.

# This software is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this software.  If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#     Manuela Kuhn <manuela.kuhn@desy.de>
#

"""Set up environment.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

from .transfer import Transfer  # noqa F401
from .transfer import generate_filepath
from .transfer import convert_suffix_list_to_regex
from .ingest import Ingest  # noqa F401
from .control import Control  # noqa F401
from .control import check_netgroup
from ._shared_utils import (LoggingFunction,
                            execute_ldapsearch,
                            NotSupported,
                            UsageError,
                            FormatError,
                            ConnectionFailed,
                            VersionError,
                            AuthenticationFailed,
                            CommunicationFailed,
                            DataSavingError)
from ._version import __version__
from ._constants import CONNECTION_LIST


__all__ = [
    "Transfer",
    "Control",
    "Ingest",
    "check_netgroup",
    "__version__",
    "CONNECTION_LIST",
    "LoggingFunction",
    "execute_ldapsearch",
    "generate_filepath",
    "convert_suffix_list_to_regex",
    "NotSupported",
    "UsageError",
    "FormatError",
    "ConnectionFailed",
    "VersionError",
    "AuthenticationFailed",
    "CommunicationFailed",
    "DataSavingError"
]
