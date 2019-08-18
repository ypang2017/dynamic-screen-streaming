#! /bin/sh

# 获取flume进程号关闭flume

home=$(cd `dirname $0`; cd ..; pwd)

. ${home}/bin/common.sh

flume_pid=`cat ${logs_home}/flume_process.pid`
autogen_data_pid=`cat ${logs_home}/autogen_data.pid`

kill -9 ${autogen_data_pid}
kill -9 ${flume_pid}
