#!/bin/sh

# 优雅关闭application
# 实现方法：停止application不是直接kill掉pid，而是启用一个监控线程，如果监控到有停止目录，在内部执行stop

home=$(cd `dirname $0`; cd ..; pwd)

. ${home}/bin/common.sh

hdfs dfs -mkdir -p /spark-streaming/transaction/stop