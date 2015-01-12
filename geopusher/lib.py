import os
import uuid
import json
import shutil
import ckanapi
import zipfile
import logging
import requests
import shapefile

TEMPDIR = '../tmp'
OUTDIR = os.path.join(TEMPDIR, 'out')

class FileTooLargeError(Exception):

    def __init__(self, extra_msg=None):
    self.extra_msg = extra_msg

    def __str__(self):
        return self.extra_msg

def convert_and_import(ckan, dataset):
    resources = dataset['resources']
    for resource in resources:
        if resource['format'] == 'SHP':
            process(ckan, resource)

def convert_and_import_all(ckan):
    for dataset in ckan.action.package_list():
        convert_and_import(ckan, dataset)

def process(ckan, resource):
    file = download_file(resource['url'])

    unzipped_dir = unzip_file(file)

    shapefile = None
    for f in os.listdir(unzipped_dir):
        if f.endswith(".shp"):
            shapefile = f

    outfile = os.path.join(OUTDIR,
                          "{0}.{1}".format(resource['name'], 'json'))

    convert_file(os.path.join(unzipped_dir, shapefile), outfile)

    if os.path.getsize(outfile) > 20000000:
        raise FileTooLargeError()

    package = ckan.action.package_show(id=resource['package_id'])
    for res in package['resources']:
        if res['format'] == 'GeoJSON' and res['name'] == resource['name']:
            ckan.action.resource_delete(id=res['id'])

    ckan.action.resource_create(
        package_id = resource['package_id'],
        upload = open(outfile),
        format = 'GeoJSON',
        name = resource['name'],
        url = 'any'
    )

def convert_file(shapefile_path, outfile_path):
    if os.path.isfile(outfile_path):
        os.remove(outfile_path)

    call(['ogr2ogr', '-f', 'GeoJSON', '-t_srs', 'crs:84',
            outfile_path, shapefile_path ])

def download_file(url):
    tmpname = '{0}.{1}'.format(uuid.uuid1(), 'shp.zip')
    response = requests.get(url, stream=True)
    with open(os.path.join(TEMPDIR, tmpname), 'wb') as out_file:
        shutil.copyfileobj(response.raw, out_file)

    return tmpname

def unzip_file(filepath):
    z = zipfile.ZipFile(os.path.join(TEMPDIR, filepath))
    dirname = os.path.join(TEMPDIR, filepath[:-4])
    os.makedirs(dirname)
    for name in z.namelist():
        z.extract(name, dirname)

    return dirname