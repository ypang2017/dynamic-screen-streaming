#! /bin/sh

# 获取主目录
home=$(cd `dirname $0`; cd ..; pwd)

# 得到各个基本目录
. ${home}/bin/common.sh

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

# 启动flume
${flume_ng} agent -n a1 -c conf/ -f ${configFile} >> ${logs_home}/flume_process.log 2>&1 &
# 把flume pid存到指定目录
echo $! > ${logs_home}/flume_process.pid

# 执行数据自动生成主函数
java -cp ${lib_home}/*:${lib_home}/*.jar com.yuyu.flume.data.AutoGenRequestMessage ${dataConfigFile} 2>&1 &
# 把数据自动生成程序pid存到指定目录
echo $! > ${logs_home}/autogen_data.pid


