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

import swiftclient

MAX_RETRIES = 10

try:
    from swiftclient.exceptions import ClientException
except ImportError:
    # swiftclient-1.4 support
    from swiftclient import ClientException


def browse_account(cnx):
    head, containers = cnx.get_account(full_listing=True)
    account_size = int(head['x-account-bytes-used'])
    # containers is a list of dicts('count', 'bytes', 'name')
    return account_size, containers


def browse_container(cnx, container):
    try:
        head, objects = cnx.get_container(container, full_listing=True)
    except(ClientException):
        # When container is somehow not available
        return 0, [], []

    container_size = int(head['x-container-bytes-used'])
    object_names = [obj['name'] for obj in objects]
    object_sizes = [int(obj['bytes']) for obj in objects]
    return container_size, object_names, object_sizes


def _get_swift_connexion(tenant,
                         bare_storage_url,
                         os_options,
                         admin_token):
    tenant_storage_url = bare_storage_url + tenant.id
    return swiftclient.client.Connection(
        authurl=None, user=None, key=None,
        preauthurl=tenant_storage_url,
        os_options=os_options,
        preauthtoken=admin_token,
        retries=MAX_RETRIES)


def _get_account_stats_dict(tenant, account_size, containers, email=""):
    container_sizes = [int(cont['bytes']) for cont in containers]
    mi = None
    ma = None
    av = None
    if containers:
        mi = min(container_sizes)
        ma = max(container_sizes)
        av = sum(container_sizes) / len(containers)
    if isinstance(tenant.name, unicode):
        name = tenant.name.encode('utf-8')
    else:
        name = tenant.name
    account_stats = {'account_name': name,
                     'account_id': tenant.id,
                     'account_size': account_size,
                     'container_amount': len(containers),
                     'container_max_size': ma,
                     'container_min_size': mi,
                     'container_avg_size': av,
                     'email': email}
    return account_stats


def _retrieve_base_account_stats(tenant,
                                 bare_storage_url,
                                 os_options,
                                 admin_token,
                                 email=""):
    cnx = _get_swift_connexion(tenant, bare_storage_url,
                               os_options, admin_token)
    account_size, containers = browse_account(cnx)
    account_stats = _get_account_stats_dict(tenant, account_size,
                                            containers, email=email)
    return cnx, containers, account_stats


def retrieve_account_stats(*args, **kwargs):
    cnx, containers, account_stats = _retrieve_base_account_stats(
        *args, **kwargs)

    containers_stats = []
    for container in containers:
        container_size, object_names, object_sizes = \
            browse_container(cnx, container['name'])
        if isinstance(container['name'], unicode):
            name = container['name'].encode('utf-8')
        else:
            name = container['name']
        mi = None
        ma = None
        av = None
        if object_names:
            mi = min(object_sizes)
            ma = max(object_sizes)
            av = sum(object_sizes) / len(object_names)
        container_details = {'container_name': name,
                             'container_size': container_size,
                             'object_sizes': object_sizes,
                             'object_amount': len(object_names),
                             'object_max_size': ma,
                             'object_min_size': mi,
                             'object_avg_size': av}
        containers_stats.append(container_details)
    return account_stats, containers_stats


def quick_retrieve_account_stats(*args, **kwargs):
    cnx, containers, account_stats = _retrieve_base_account_stats(
        *args, **kwargs)

    containers_stats = []
    for cont in containers:
        name = cont['name'].encode('utf-8') if isinstance(
            cont['name'], unicode) else cont['name']
        obj_avg = cont['bytes'] / cont['count'] \
                  if cont['count'] != 0 else None
        container_details = {'container_name': name,
                             'container_size': cont['bytes'],
                             'object_sizes': [],
                             'object_amount': cont['count'],
                             'object_max_size': None,
                             'object_min_size': None,
                             'object_avg_size': obj_avg}
        containers_stats.append(container_details)
    return account_stats, containers_stats
