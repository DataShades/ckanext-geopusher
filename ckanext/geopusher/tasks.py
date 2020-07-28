from __future__ import print_function
from __future__ import absolute_import
from requests import get
from .lib import process

import ckanapi

try:
    from ckan.lib.celery_app import celery

    @celery.task(name='geopusher.process_resource')
    def process_resource(*args, **kwargs):
        return process_resource_task(*args, **kwargs)

except ImportError:
    pass


def process_resource_task(resource_id, site_url, apikey):
    ckan = ckanapi.RemoteCKAN(site_url, apikey=apikey)
    print("processing resource {0}".format(resource_id))
    process(ckan, resource_id)
