#!/bin/sh

deleteCheckpoint=$1

# 获取主目录
home=$(cd `dirname $0`; cd ..; pwd)

# 得到各个基本目录
. ${home}/bin/common.sh

# 删除控制stop的目录，否则启动后，应用会自动关闭
hdfs dfs -rm -r /spark-streaming/transaction/stop

# 如果应用代码有更新，启动前要删除上一次的checkpoint目录，否则启动不成功
if [ "$deleteCheckpoint" == "delck" ]; then
    hdfs dfs -rm -r /spark-streaming/checkpoint/transaction
fi

# 添加依赖的jar包
jars=""

for file in ${lib_home}/*
do

    if [ -z ${jars} ]; then
        jars=${file}
    else
        jars="${jars},${file}"
    fi
done

# 启动application
${spark_submit} \
--master yarn \
--deploy-mode client \
--name ScreenMessageStreaming \
--driver-memory 512M \
--executor-memory 512M \
--jars ${jars} \
--class com.yuyu.stream.spark.streaming.ScreenMessageStreaming \
${lib_home}/screen-display-1.0-SNAPSHOT.jar ${configFile} \
>> ${logs_home}/screen_stream.log 2>&1 &

# 把pid存到指定目录
echo $! > ${logs_home}/screen_stream.pid
