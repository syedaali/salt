'''
Connection module for AWS Cloud Directory
:maintainer:    Syed Ali alicsyed@gmail.com
:maturity:      new
:depends:       boto
:platform:      all

If a region is not specified, the default is us-east-1.

'''
from __future__ import absolute_import
import logging

import salt.utils.path
import salt.utils.files
from salt.exceptions import SaltInvocationError


log = logging.getLogger(__name__)

__func_alias__ = {
    'list_': 'list',
}

__virtualname__ = 'boto_clouddirectory'

try:
    import boto3
    import botocore

    logging.getLogger('boto3').setLevel(logging.CRITICAL)
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False


def __virtual__():
    '''
     Require boto.

    '''
    if not HAS_BOTO3:
        return (False, 'The boto_ds module could not be loaded: boto3 libraries not found')
    __utils__['boto3.assign_funcs'](__name__, 'clouddirectory')
    return True


def create_schema(name, region=None, key=None, keyid=None, profile=None):
    '''
    Create a schema in development state.
    Returns: The ARN of the the schema in development state.

    CLI Example:

    .. code-block:: bash

            salt myminion boto_clouddirectory.create_schema dev_schema_name region=us-west-2
    '''
    conn = _get_conn(region=region, key=key, keyid=keyid, profile=profile)

    if not name:
        raise SaltInvocationError('Must specify schema name to use')

    try:
        create_s = conn.create_schema(Name=name)
    except botocore.exceptions.ClientError as e:
        return {'error': __utils__['boto3.get_error'](e)}

    return create_s['SchemaArn']


def publish_schema(arn, version, region=None, key=None, keyid=None, profile=None):
    '''
    Publish an existing schema from development state to published state.
    Returns: The ARN of the schema published.

    CLI Example:

    .. code-block:: bash

        salt myminion boto_clouddirectory.publish_schema arn:aws:clouddirectory:us-west-2:123456789:schema/development/testing '1.0' region=us-west-2
    '''
    conn = _get_conn(region=region, key=key, keyid=keyid, profile=profile)

    if not arn or not version:
        raise SaltInvocationError('Must specify development schema ARN and version to use')

    try:
        publish_s = conn.publish_schema(DevelopmentSchemaArn=arn, Version=str(version))
    except botocore.exceptions.ClientError as e:
        return {'error': __utils__['boto3.get_error'](e)}

    return publish_s['PublishedSchemaArn']


def create_directory(name, arn, region=None, key=None, keyid=None, profile=None):
    '''
    Create a cloud directory.
    Returns: The ARN of the directory created.

    CLI Example:

    .. code-block:: bash

        salt myminion boto_clouddirectory.create_schema 'directory_name' region=us-west-2
    '''

    conn = _get_conn(region=region, key=key, keyid=keyid, profile=profile)

    if not name or not arn:
        raise SaltInvocationError('Must specify directory name and schema ARN')

    try:
        create_d = conn.create_directory(Name=name, SchemaArn=arn)
    except botocore.exceptions.ClientError as e:
        return {'error': __utils__['boto3.get_error'](e)}

    return create_d['DirectoryArn']


def _filedata(infile):
    '''
     Private function to read from JSON file.
     Returns: The contents of the file
     '''
    with salt.utils.fopen(infile, 'rb') as f:
        return f.read()


def put_schema_from_json(arn, document, region=None, key=None, keyid=None, profile=None):
    '''
    Update an existing development state schema with a new schema that is defined in
    a JSON document.
    Returns: The ARN of the schema that was updated.

    CLI Example:

    .. code-block:: bash

        salt myminion boto_clouddirectory.put_schema_from_json arn:aws:clouddirectory:us-west-2:123456789:schema/development/testing /srv/salt/files/document.json region=us-west-2
    '''
    conn = _get_conn(region=region, key=key, keyid=keyid, profile=profile)

    if not arn or not document:
        raise SaltInvocationError('Must specify schema ARN and JSON document')

    json_document = _filedata(document)
    try:
        put_schema = conn.put_schema_from_json(SchemaArn=arn, Document=json_document)
    except botocore.exceptions.ClientError as e:
        return {'error': __utils__['boto3.get_error'](e)}

    return put_schema['Arn']


def delete_schema(arn, region=None, key=None, keyid=None, profile=None):
    '''
    Delete a schema.
    Returns: The ARN of the schema that was deleted.

    CLI Example:

    .. code-block:: bash

        salt myminion boto_clouddirectory.delete_schema arn:aws:clouddirectory:us-west-2:123456789:schema/development/testing region=us-west-2
    '''
    conn = _get_conn(region=region, key=key, keyid=keyid, profile=profile)

    if not arn:
        raise SaltInvocationError('Must specify schema ARN to delete')

    try:
        create_d = conn.delete_schema(SchemaArn=arn)
    except botocore.exceptions.ClientError as e:
        return {'error': __utils__['boto3.get_error'](e)}

    return arn


def list_development_schema_arns(region=None, key=None, keyid=None, profile=None):
    '''
    List development state schema ARN(s).
    Returns: A list of development state schema ARN(s) if available.

    CLI Example:

    .. code-block:: bash

        salt myminion boto_clouddirectory.list_development_schema_arns region=us-west-2
    '''
    conn = _get_conn(region=region, key=key, keyid=keyid, profile=profile)
    arn_list = []
    try:
        dev_schemas = conn.list_development_schema_arns()
        for arn in dev_schemas['SchemaArns']:
            arn_list.append(arn)
    except botocore.exceptions.ClientError as e:
        return {'error': __utils__['boto3.get_error'](e)}

    return arn_list


def list_published_schema_arns(region=None, key=None, keyid=None, profile=None):
    '''
    List published state schema ARN(s).
    Returns: A list of published state schema ARN(s) if available.

    CLI Example:

    .. code-block:: bash

        salt myminion boto_clouddirectory.list_published_schema_arns region=us-west-2
    '''
    conn = _get_conn(region=region, key=key, keyid=keyid, profile=profile)
    arn_list = []
    try:
        pub_schemas = conn.list_published_schema_arns()
        for arn in pub_schemas['SchemaArns']:
            arn_list.append(arn)
    except botocore.exceptions.ClientError as e:
        return {'error': __utils__['boto3.get_error'](e)}

    return arn_list
