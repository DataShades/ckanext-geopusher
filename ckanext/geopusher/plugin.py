import ckan.plugins as plugins
import ckan.model as model

import ckantoolkit as toolkit

import uuid

import ckanapi

from ckan.model.domain_object import DomainObjectOperation
from ckan.plugins.toolkit import get_action


def _compat_enqueue(name, fn, args=None):
    u"""
    Enqueue a background job using Celery or RQ.
    """
    try:
        # Try to use RQ
        from ckan.plugins.toolkit import enqueue_job

        enqueue_job(fn, args=args)
    except ImportError:
        # Fallback to Celery
        import uuid
        from ckan.lib.celery_app import celery

        celery.send_task(name, args=args, task_id=str(uuid.uuid4()))


class GeopusherPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IDomainObjectModification, inherit=True)

    # IConfigurer

    def update_config(self, config_):
        toolkit.add_template_directory(config_, 'templates')
        toolkit.add_public_directory(config_, 'public')
        toolkit.add_resource('fanstatic', 'geopusher')

    def notify(self, entity, operation=None):
        import ckanext.geopusher.tasks as tasks
        if isinstance(entity, model.Resource):
            resource_id = entity.id
            # new event is sent, then a changed event.
            if operation == DomainObjectOperation.changed:
                # There is a NEW or CHANGED resource. We should check if
                # it is a shape file and pass it off to Denis's code if
                # so it can process it
                site_url = toolkit.config.get('ckan.site_url', 'http://localhost/')
                apikey = model.User.get('default').apikey

                _compat_enqueue(
                    'geopusher.process_resource',
                    tasks.process_resource_task,
                    [resource_id, site_url, apikey])
