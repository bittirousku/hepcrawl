# -*- coding: utf-8 -*-
#
# This file is part of hepcrawl.
# Copyright (C) 2015, 2016 CERN.
#
# hepcrawl is a free software; you can redistribute it and/or modify it
# under the terms of the Revised BSD License; see LICENSE file for
# more details.

"""Version information for hepcrawl.

This file is imported by ``hepcrawl.__init__``,
and parsed by ``setup.py``.
"""

from __future__ import absolute_import, print_function

import os

from autosemver.packaging import get_current_version

__version__ = get_current_version(
    project_name='hepcrawl',
    project_dir=os.path.join(os.path.dirname(__file__), '..')
)
