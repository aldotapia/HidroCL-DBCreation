#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2022 Aldo Tapia.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, see <http://www.gnu.org/licenses/>.
#
# Check https://nsidc.github.io/earthdata/ for earthdata python
# library installation

import time
from datetime import datetime

import hidrocl_paths as hcl
from hidrocl import remove_duplicates

remove_duplicates(hcl.modis_test_path)

#remove_duplicates(hcl.mcd12q1_path)
#remove_duplicates(hcl.mcd15a2h_path)
#remove_duplicates(hcl.mcd43a3_path)
#remove_duplicates(hcl.mod10a2_path)
#remove_duplicates(hcl.mod13q1_path)
#remove_duplicates(hcl.mod16a2_path)
