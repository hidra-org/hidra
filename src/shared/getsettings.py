#!/usr/bin/env python

from __future__ import print_function
import helpers

CONFIG_FILE = "/opt/hidra/conf/datamanager.conf"

params = helpers.parse_parameters(helpers.read_config(CONFIG_FILE))["asection"]

print ("Configured subdirectories are:", params["fix_subdirs"])
