#!/bin/bash

#mvn clean package -DskipTests
mvn clean package -Pdist,native -DskipTests -Dtar -Denforcer.skip=true -Drequire.pmdk -Disal.lib=/usr/lib64/ -Dbundle.isal=true

if [ "$?" -ne "0" ]; then
  echo "mvn bulid failed!"
  exit 255
fi

base_path=$(
  cd $(dirname $0)
  pwd
)

pkg_dir=$base_path/package

os_ver=$(uname -s)

project_name="dap/hadoop"
git_branch_name=$(git rev-parse --abbrev-ref HEAD)
git_last_commit_id=$(git rev-parse HEAD)

hadoop_version=$(grep "<hadoop.version>" pom.xml | awk -F '>' '{print $2}' | awk -F '<' '{print $1}')

function versioninfo_mac {
  dirpath=$2
  tarpath=$3

  hadoop_md5_num=$(md5 $dirpath/$tarpath | awk -F '= ' '{print $2}')
  echo "{\"project\":\"$project_name\",\"path\":\"hadoop\", \"branch\":\"$git_branch_name\", \"commit\":\"$git_last_commit_id\", \"md5\":\"$hadoop_md5_num\"}" >$dirpath/version.info
}

function versioninfo_linux {
  dirpath=$1
  tarpath=$2

  hadoop_md5_num=$(md5sum $dirpath/$tarpath | awk -F ' ' '{print $1}')
  echo "{\"project\":\"$project_name\",\"path\":\"hadoop\", \"branch\":\"$git_branch_name\", \"commit\":\"$git_last_commit_id\", \"md5\":\"$hadoop_md5_num\"}" >$dirpath/version.info
}

hadoop_dir_name=hadoop-$hadoop_version
hadoop_ranger_dir_name=hadoop-ranger-$hadoop_version

hadoop_tar_name=hadoop-$hadoop_version.tar.gz
hadoop_ranger_tar_name=hadoop-ranger-$hadoop_version.tar.gz

rm -rf ${pkg_dir}/*
mkdir -p ${pkg_dir}/$hadoop_dir_name
mkdir -p ${pkg_dir}/$hadoop_ranger_dir_name

cp -r $base_path/hadoop-dist/target/$hadoop_tar_name $pkg_dir/$hadoop_dir_name/
cp -r $base_path/hadoop-dist/target/$hadoop_dir_name $pkg_dir/$hadoop_ranger_dir_name/$hadoop_ranger_dir_name

cd $pkg_dir/$hadoop_ranger_dir_name/$hadoop_ranger_dir_name/share/hadoop/hdfs/lib/
ln -nfs /opt/hadoop/gateway/bzlhdfsrangerplugin/current/lib/bzl-ranger.jar bzl-ranger.jar

cd $pkg_dir/$hadoop_ranger_dir_name
tar -zcvf $hadoop_ranger_tar_name $hadoop_ranger_dir_name

if [[ "$os_ver" =~ "Darw" ]]; then
  versioninfo_mac $pkg_dir/$hadoop_dir_name $hadoop_tar_name
  versioninfo_mac $pkg_dir/$hadoop_ranger_dir_name $hadoop_ranger_tar_name
else
  versioninfo_linux $pkg_dir/$hadoop_dir_name $hadoop_tar_name
  versioninfo_linux $pkg_dir/$hadoop_ranger_dir_name $hadoop_ranger_tar_name
fi

rm -rf  $base_path/hadoop-dist/target/$hadoop_dir_name $pkg_dir/$hadoop_ranger_dir_name/$hadoop_ranger_dir_name

cd $base_path/package
## 在package目录下完成打包操作
# shellcheck disable=SC2045
for file in $(ls $base_path/package); do
  if [ -d $file ]; then
    tar -zcvf $file.tar.gz $file
    rm -rf $base_path/package/$file
  fi
done