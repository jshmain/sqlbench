#!/bin/bash

# Create folder structure
if [ $# -ne 1 ]
then
	echo "Usage: $0 <release version>"
        exit 1
fi

export release_dir=$1
mkdir $release_dir

cp -r configs $release_dir
cp -r python $release_dir
cp -r suites $release_dir


tar -cvf  $release_dir.tar $release_dir
gzip $release_dir.tar
rm -rf $release_dir
