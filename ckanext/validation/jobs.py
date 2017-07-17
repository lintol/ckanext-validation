# encoding: utf-8

import logging
import datetime
import json

from goodtables import Inspector

from ckan.model import Session
import ckan.lib.uploader as uploader

from ckanext.validation.model import Validation


log = logging.getLogger(__name__)


def run_validation_job(resource):

    log.debug(u'Validating resource {}'.format(resource['id']))

    validation = Session.query(Validation).filter(
        Validation.resource_id == resource['id']).one_or_none()

    if not validation:
        validation = Validation(resource_id=resource['id'])

    validation.status = u'running'
    Session.add(validation)
    Session.commit()

    source = None
    if resource.get(u'url_type') == u'upload':
        upload = uploader.get_resource_uploader(resource)
        if isinstance(upload, uploader.ResourceUpload):
            source = upload.get_path(resource[u'id'])
    if not source:
        source = resource[u'url']

    schema = resource.get(u'schema')
    if schema and isinstance(schema, basestring):
        schema = json.loads(schema)

    _format = resource[u'format'].lower()

    report = _validate_table(source, _format=_format, schema=schema)

    if report['table-count'] > 0:
        validation.status = u'success' if report[u'valid'] else u'failure'
        validation.report = report
    else:
        validation.status = u'error'
        validation.error = {
            'message': '\n'.join(report['warnings']) or u'No tables found'}
    validation.finished = datetime.datetime.utcnow()
    Session.add(validation)
    Session.commit()


def _validate_table(source, _format=u'csv', schema=None):

    inspector = Inspector()

    report = inspector.inspect(source, format=_format, schema=schema)

    log.debug(u'Source: %s' % source)

    return report
