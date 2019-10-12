#!/bin/bash

source ~/.bashrc

service_id=`ps -ef | grep mysqltoes-1.0.jar | grep -v grep | awk '{print $2}'`
base_path=$(cd `dirname $0`; pwd)
cd $base_path
cd ..
pwd

if [ "$service_id" == "" ];then
  echo `date '+%Y%m%d %H:%M:%S'`" start again MysqlToEs Servie"
  nohup java -jar lib/mysqltoes-1.0.jar &
else
  #echo $service_id
  echo `date '+%Y%m%d %H:%M:%S'`" MysqlToEs Servie is perfect"
fi
