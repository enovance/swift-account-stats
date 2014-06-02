#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (C) 2013 eNovance SAS <licensing@enovance.com>
#
# Author: Fabien Boucher <fabien.boucher@enovance.com>
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import csv
import sys

# Nicer filesize reporting make it optional
try:
    import hurry.filesize
    prettysize = hurry.filesize.size
except ImportError:
    prettysize = None


def csv_write(fd, o_order, parsed_stats):
    writer = csv.DictWriter(fd, o_order, extrasaction='ignore')
    writer.writeheader()
    writer.writerows(parsed_stats)


def prettyfy_size(parsed_stats, raw_output):
    if prettysize and not raw_output:
        temp_stats = []
        for s in parsed_stats:
            t_s = {}
            for k, v in s.iteritems():
                if k.endswith('_size'):
                    t_s[k] = prettysize(v and float(v) or 0)
                else:
                    t_s[k] = v
            temp_stats.append(t_s)
        return temp_stats
    return parsed_stats


def report_detailed_stats(stats, raw_output, path=None):
    o_order = ('email', 'account_name', 'account_id', 'account_size',
               'container_amount', 'container_max_size',
               'container_min_size', 'container_avg_size',
               'container_name', 'container_size',
               'object_amount', 'object_max_size',
               'object_min_size', 'object_avg_size')
    parsed_stats = []
    for xstat in stats:
        account_stats = xstat[0]
        containers_stats = xstat[1]
        for cstat in containers_stats:
            s = {}
            s.update(account_stats)
            s.update(cstat)
            parsed_stats.append(s)
    parsed_stats = prettyfy_size(parsed_stats, raw_output)
    if not path:
        fd = sys.stdout
        csv_write(fd, o_order, parsed_stats)
    else:
        with open(path, "w") as fd:
            csv_write(fd, o_order, parsed_stats)
        fd.close()


def report_global_stats(stats, raw_output, path=None):
    o_order = ('account_amount', 'account_max_size', 'account_min_size',
               'account_avg_size', 'total_size', 'container_amount',
               'container_max_size', 'container_min_size',
               'container_avg_size', 'object_amount', 'object_max_size',
               'object_min_size', 'object_avg_size')
    account_sizes = []
    total_objects_count = 0
    container_sizes = []
    object_sizes = []
    a_am = 0
    t_s = 0
    c_am = 0
    for xstat in stats:
        account_stats = xstat[0]
        containers_stats = xstat[1]
        a_am += 1
        t_s += account_stats['account_size']
        account_sizes.append(account_stats['account_size'])
        for cstat in containers_stats:
            c_am += 1
            container_sizes.append(cstat['container_size'])
            object_sizes.extend(cstat['object_sizes'])
            total_objects_count += cstat['object_amount']
    a_ma_s = (len(account_sizes) == 0) and -1 or max(account_sizes)
    a_mi_s = (len(account_sizes) == 0) and -1 or min(account_sizes)
    a_avg_s = (len(account_sizes) == 0) and -1 or (sum(account_sizes) / a_am)
    c_ma_s = (len(container_sizes) == 0) and -1 or max(container_sizes)
    c_mi_s = (len(container_sizes) == 0) and -1 or min(container_sizes)
    c_avg_s = (len(container_sizes) == 0) and -1 or \
              (sum(container_sizes) / c_am)
    o_am = total_objects_count
    o_ma_s = (len(object_sizes) == 0) and -1 or max(object_sizes)
    o_mi_s = (len(object_sizes) == 0) and -1 or min(object_sizes)
    o_avg_s = float(t_s) / total_objects_count
    parsed_stats = dict([('account_amount', a_am),
                        ('account_max_size', a_ma_s),
                        ('account_min_size', a_mi_s),
                        ('account_avg_size', a_avg_s),
                        ('total_size', t_s),
                        ('container_amount', c_am),
                        ('container_max_size', c_ma_s),
                        ('container_min_size', c_mi_s),
                        ('container_avg_size', c_avg_s),
                        ('object_amount', o_am),
                        ('object_max_size', o_ma_s),
                        ('object_min_size', o_mi_s),
                        ('object_avg_size', o_avg_s)])
    parsed_stats = prettyfy_size([parsed_stats, ], raw_output)
    if not path:
        fd = sys.stdout
        csv_write(fd, o_order, parsed_stats)
    else:
        with open(path, "w") as fd:
            csv_write(fd, o_order, parsed_stats)
        fd.close()
