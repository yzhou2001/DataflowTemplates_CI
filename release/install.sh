#!/bin/bash
DIR=`dirname "$0"`
DIR="$( cd "$(dirname "$0")" ; pwd -P )"

OUTDIR=generated
if [ -d $OUTDIR ]; then
  rm -rf $OUTDIR
elif [ -f $OUTDIR ]; then
  rm -f $OUTDIR
fi

cd $DIR
# Protocol buffer compilation
mkdir generated
protoc -I=$DIR --python_out=$OUTDIR $DIR/template_release_info.proto $DIR/template_ui_metadata.proto $DIR/templates.proto

# Stage the templates
export PYTHONPATH=$DIR/$OUTDIR
python3 template_release.py  --template_staging_bucket=gs://yazhou_reddit1/staging_templates --library_staging_bucket=gs://yazhou_reddit1/staging_libs --project=yazhou-test1 --candidate_name=pre-release --release_type=STAGING --include_hidden=false

# Clean up
rm -rf $OUTDIR
