"""template_release is the mechanism which releases Cloud Teleport pipelines.

Pipelines are specified in template_release.info.textproto. While pipelines
themselves are in //third_party/java_src/cloud/teleport.

It is used by the Dataflow Template Release Rapid process to release these
templates to production. The locations for the released templates are:
  Template:
  <template_dir>/<release_name>/<name>
  Staged jars:
  <staging_dir>/<release_name>/...

Worth noting is that 'staging' is an overloaded term in the context of template
releases. It can refer to both a) the release step before production, and b) the
artifacts stored in GCS by a template pipeline. Careful attention should be paid
to context to ensure the correct 'staging' is being understood.
  Staging Release / Environment: Release step before production.
  Staging Libraries: Libraries uploaded to GCS for a template pipeline.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from collections import OrderedDict

import json
import os
import subprocess
import argparse

#  Redacted google import paths
from template_release_info_pb2 import TemplateReleaseCategories, TemplateReleaseCategory
from template_ui_metadata_pb2 import TemplateCategories
from google.protobuf import json_format, text_format
from tensorflow import app, flags, gfile
import logging

STAGING_RELEASE_NAME = 'STAGING'
PRODUCTION_RELEASE_NAME = 'PROD'
parser = argparse.ArgumentParser(description='Google Dataflow Template Release')
parser.add_argument('--template_staging_bucket', required=True)
parser.add_argument('--library_staging_bucket', required=True)
parser.add_argument('--project', required=True)
parser.add_argument('--candidate_name', required=True)
parser.add_argument('--release_type', required=True)
parser.add_argument('--include_hidden', type=bool, required=True)
parsed_args = parser.parse_args()


def setFlags(template_staging_bucket, library_staging_bucket, project,
    candidate_name, include_hidden):
  flags.DEFINE_string('template_prod_bucket', '', 'Bucket in which to store '
                                                  'production templates. Should begin with "gs://".')
  flags.DEFINE_string('template_staging_bucket', template_staging_bucket, 'Bucket in which to store '
                                                                          'staged templates. Should begin with "gs://".')
  flags.DEFINE_string('library_staging_bucket', library_staging_bucket,
                      'Bucket in which to store staged jar files.'
                      ' Should begin with "gs://".')
  flags.DEFINE_string('project', project,
                      'GCP Project in which to create the template pipelines.')
  flags.DEFINE_string('candidate_name', candidate_name, 'Name of release candidate.')
  flags.DEFINE_string('release_name', '',
                      'Name of release -- used to version templates.')
  flags.DEFINE_string('metadata_dir_override', '', 'Directory for metadata files')

  flags.DEFINE_bool('include_hidden', include_hidden,
                    'Include hidden templates in the release.')


setFlags(parsed_args.template_staging_bucket,
         parsed_args.library_staging_bucket,
         parsed_args.project,
         parsed_args.candidate_name,
         parsed_args.include_hidden)

flags.DEFINE_enum(
    'release_type',
    None,
    [STAGING_RELEASE_NAME, PRODUCTION_RELEASE_NAME],
    'Defines whether to release templates to staging or copy staged templates '
    'to prod.')

FLAGS = flags.FLAGS
FLAGS.release_type = parsed_args.release_type

TELEPORT_PACKAGES_PER_BEAM_VERSION = {
    'live':
        'target/google-cloud-teleport-java-0.1-SNAPSHOT.jar',
}
CFG_FILE = ('template_release_info.prototext')
JAVA_BIN_PATH = 'java'
LATEST_FOLDER_NAME = 'latest'

TELEPORT_LABEL_KEY_TEMPLATE_NAME = 'goog-dataflow-provided-template-name'
TELEPORT_LABEL_KEY_TEMPLATE_VERSION = 'goog-dataflow-provided-template-version'

CATEGORIES_CONFIG_FILE_NAME = 'ui_metadata/categories.json'

TEMPLATE_GENERATION_TIMEOUT = 180  # seconds


def gcs_to_bigstore_path(gs_path):
  """Converts a gs:// path to a /bigstore/ path.

  Args:
    gs_path: Path to convert from gcs to bigstore.
  Raises:
    ValueError: If provided path is not a gs:// path.
  Returns:
    Provided path with gs:// replaced by /bigstore/.
  """

  if not gs_path.startswith('gs://'):
    raise ValueError('Path must start with `gs://`: {}.'.format(gs_path))
  return gs_path.replace('gs://', '/bigstore/')

def CopyRecursively(src, dst, overwrite=False):
  entries = gfile.ListDirectory(src)
  for entry in entries:
    src_path = os.path.join(src, entry)
    dst_path = os.path.join(dst, entry)
    if gfile.IsDirectory(src_path):
      gfile.MkDir(dst_path)
      CopyRecursively(src_path, dst_path, overwrite)
    else:
      gfile.Copy(src_path, dst_path, overwrite)

def write_production():
  """Copies staged templates to production directory.

  This function assumes that the template and associated metadata files are
  stored in a folder of the form gs://<template_staging_bucket>/<release_name>.
  It copies the templates from the <release_name> folder to two new locations:
  gs://<prod_bucket>/<release_name> and gs://<prod_bucket>/latest. Both
  folders contain identical contents; the <release_name> bucket is to allow
  customers to pin to a specific release and the `latest` folder gives the UI
  a location at which to point.

  Raises:
    GOSError if there was an error reading or writing a file.
  """
  prod_root = FLAGS.template_prod_bucket
  template_staging_root = FLAGS.template_staging_bucket

  template_dir = os.path.join(template_staging_root, FLAGS.candidate_name)
  if not gfile.IsDirectory(template_dir):
    logging.fatal('Template staging directory %s does not exist or is not a '
                  'directory.', template_dir)

  release_dir = os.path.join(prod_root, FLAGS.release_name)
  if gfile.IsDirectory(release_dir):
    logging.fatal(
        'Template release directory %s already exists. Aborting.', template_dir)

  logging.info('Copying folder from %s to %s.', template_dir, release_dir)
  gfile.MkDir(release_dir)
  CopyRecursively(template_dir, release_dir)

  # TODO: If we ever delete templates, they will stick around in
  # `latest`; evaluate something rsync-like in the future.
  latest_dir = os.path.join(prod_root, LATEST_FOLDER_NAME)
  if gfile.Exists(latest_dir):
    if not gfile.IsDirectory(latest_dir):
      gfile.Remove(latest_dir)
      gfile.MkDir(latest_dir)
  else:
    gfile.MkDir(latest_dir)

  logging.info('Copying folder from %s to %s.', template_dir, latest_dir)
  CopyRecursively(template_dir, latest_dir, overwrite=True)


def build_cmd(packages, main_class, args):
  """build_cmd puts together the java invocation for a template pipeline.

  Args:
    packages: list of packages in the classpath.
    main_class: Fully qualified name for the main class.
    args: string to string dict of arguments.

  Returns:
    Command array for the particular pipeline.
  """
  args_segments = ['--%s=%s' % ((k, v)) for (k, v) in sorted(args.items())]
  java_binary = JAVA_BIN_PATH
  return [java_binary, '-cp', ':'.join(packages), main_class] + args_segments


def write_metadata(filename, metadata):
  bigstore_filename = filename
  if filename.startswith('gs://'):
    bigstore_filename = filename
  if FLAGS.metadata_dir_override:
    bigstore_filename = os.path.join(FLAGS.metadata_dir_override,
                                     os.path.split(bigstore_filename)[-1])
  with gfile.GFile(bigstore_filename, 'w') as f:
    f.write(json_format.MessageToJson(metadata))


def write_category_information(base_destination, categories_from_config):
  """Format&write category info in JSON format under specified directory."""
  template_categories = TemplateCategories()
  for category_from_config in categories_from_config:
    template_category = template_categories.categories.add()
    template_category.category.CopyFrom(category_from_config.category)
    for template_from_config in category_from_config.templates:
      template = template_category.templates.add()
      template.name = template_from_config.name
      template.display_name = template_from_config.display_name
  filename = os.path.join(base_destination, CATEGORIES_CONFIG_FILE_NAME)
  write_metadata(filename, template_categories)


def stage_template(base_destination, template):
  """stage_template takes a config and runs it with the appropriate params.

  The result of running stage_template is that several artifacts are pushed to
  FLAGS.template_staging_bucket and FLAGS.library_staging_dir. Running the
  template generating pipeline puts the template definition in
  <template_staging_bucket>/<release_name>/<template_name> and the necessary
  libraries in <library_staging_bucket>/<release_name>. After the pipeline is
  done, the metadata specified in the config file is written to the same
  directory as the template definition.

  Args:
    base_destination: Directory for templates.
    template: TemplateReleaseInfo proto for a template.

  Returns:
    True if execution succeeded; false otherwise.
  """
  # Constructs commandline.
  main_class = template.main_class
  name = template.name
  logging.info('Releasing template %s to staging bucket.', name)
  logging.info('Template spec: %s', template)
  # Template destination:
  # <template_staging_bucket>/<release>/<template_name>.
  # E.g.,
  # gs://templates-staging/20170101_RC00/PubsubToBigQuery
  template_location = os.path.join(base_destination, name)
  labels = OrderedDict([
      (TELEPORT_LABEL_KEY_TEMPLATE_NAME, template.name.lower()),
      (TELEPORT_LABEL_KEY_TEMPLATE_VERSION, FLAGS.candidate_name.lower()),
  ])
  labels_str = json.dumps(labels).replace(' ', '')
  args = {
      'runner':
          'DataflowRunner',
      'project':
          FLAGS.project,
      'stagingLocation':  # Staged Library Location
          os.path.join(FLAGS.library_staging_bucket, FLAGS.candidate_name),
      'templateLocation':
          template_location,
      'labels':
          labels_str,
  }
  for param in template.parameters:
    args[param.key] = param.value
  base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
  packages = [
      os.path.join(
          base_dir,
          TELEPORT_PACKAGES_PER_BEAM_VERSION[template.beam_version_override])
  ]
  cmd = build_cmd(packages, main_class, args)

  new_env = os.environ.copy()

  # Executes the commandline and generate the template.
  logging.info('Executing command %s', cmd)
  # TODO: Temporary disable piping of stdout and stderr since it
  # stucks sometimes.
  try:
    ret = subprocess.run(
       cmd, timeout=TEMPLATE_GENERATION_TIMEOUT, env=new_env)
  except Exception as e:
    logging.error('Template execution threw exception %s.', str(e))
    return False
  if ret.returncode != 0:
   logging.error('Template execution failed with return code %s.', ret)
   return False
  # Writes metadata file.
  metadata_filename = '{}_metadata'.format(template_location)
  logging.info('Writing metadata file %s', metadata_filename)
  write_metadata(metadata_filename, template.metadata)
  return True


def remove_hidden_templates(categories):
  """Remove all hidden templates from categories."""
  new_categories = TemplateReleaseCategories()
  for category in categories.categories:
    new_category = TemplateReleaseCategory()
    new_category.category.CopyFrom(category.category)
    for template in category.templates:
      if template.hidden:
        logging.info('Template %s is hidden, skipped.', template.name)
        continue
      new_category.templates.add().CopyFrom(template)
    if not new_category.templates:
      logging.info('Empty category %s skipped', new_category.category.name)
      continue
    new_categories.categories.add().CopyFrom(new_category)
  return new_categories


def main(unused_args):
  if FLAGS.release_type == PRODUCTION_RELEASE_NAME:
    return write_production()
  elif FLAGS.release_type == STAGING_RELEASE_NAME:
    base_destination = os.path.join(FLAGS.template_staging_bucket,
                                    FLAGS.candidate_name)
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, CFG_FILE)
    config_categories = TemplateReleaseCategories()
    with open(config_path, 'r') as f:
      text_format.Merge(f.read(), config_categories)

    if not FLAGS.include_hidden:
      config_categories = remove_hidden_templates(config_categories)

    write_category_information(base_destination, config_categories.categories)
    for category in config_categories.categories:
      for template in category.templates:
        if not stage_template(base_destination, template):
          logging.error('Template %s failed to release', template.name)
          return 1
  else:
    logging.fatal('Invalid release type specified: release_type=%s.',
                  FLAGS.release_type)


def validate_library_flag(flag):
  if FLAGS.release_type == PRODUCTION_RELEASE_NAME:
    return not flag
  elif FLAGS.release_type == STAGING_RELEASE_NAME:
    return flag.startswith('gs://')


def validate_prod_flag(flag):
  if FLAGS.release_type == PRODUCTION_RELEASE_NAME:
    return flag.startswith('gs://')
  elif FLAGS.release_type == STAGING_RELEASE_NAME:
    return not flag

if __name__ == '__main__':
  flags.register_validator(
      'template_staging_bucket',
      lambda v: v.startswith('gs://'),
      message='template_staging_bucket should start with gs://')

  flags.register_validator(
      'library_staging_bucket',
      validate_library_flag,
      message=('library_staging_bucket should only be specified with a STAGING '
               'release and should start with gs://'))

  flags.register_validator(
      'candidate_name',
      lambda v: v,
      message='must specify a release candidate name')

  flags.register_validator(
      'release_name',
      lambda v: v if FLAGS.release_type == PRODUCTION_RELEASE_NAME else not v,
      message='must specify a release name only if releasing to production.')

  flags.register_validator(
      'release_type',
      lambda v: v,
      message='must specify a release type')

  flags.register_validator(
      'template_prod_bucket',
      validate_prod_flag,
      message=('template_prod_bucket should only be specified with PROD release'
               ' and should start with gs://'))

  app.run(main)
