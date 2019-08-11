package com.yuyu.stream.spark.streaming;

import com.yuyu.stream.common.utils.DateUtils;
import com.yuyu.stream.common.utils.JSONUtil;
import com.yuyu.stream.common.utils.MyStringUtil;
import com.yuyu.stream.common.utils.PropertiesUtil;
import com.yuyu.stream.spark.key.UserHourSystemKey;
import com.yuyu.stream.spark.model.ConsumeOffsetModel;
import com.yuyu.stream.spark.model.SingleUserTransactionRequestModel;
import com.yuyu.stream.spark.model.UserTransactionAmountModel;
import com.yuyu.stream.spark.model.UserTransactionRequestModel;
import com.yuyu.stream.spark.service.ConsumeOffsetService;
import com.yuyu.stream.spark.service.impl.MysqlTransactionAmountService;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaCluster;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConversions;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author yuyu
 */
public class ScreenMessageStreaming {
    public static final Logger log = Logger.getLogger(ScreenMessageStreaming.class);

    public static void main(String[] args) throws InterruptedException, IOException {
        if (args.length < 1) {
            System.err.println("Usage: ScreenMessageStreaming ConfigFile");
            System.exit(1);
        }
        //获取配置文件路径
        String configFile = args[0];
        final Properties serverProps = PropertiesUtil.getProperties(configFile);
        //获取checkpoint的hdfs路径
        String checkpointPath = serverProps.getProperty("streaming.checkpoint.path");
        //如果checkpointPath hdfs目录下有文件，则反序列化文件生成context,否则使用函数createContext返回的context对象
        JavaStreamingContext javaStreamingContext = JavaStreamingContext.getOrCreate(checkpointPath, createContext(serverProps));
        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();

    }

    private static Function0<JavaStreamingContext> createContext(final Properties serverProps) {
        Function0<JavaStreamingContext> createContextFunc = new Function0<JavaStreamingContext>() {
            @Override
            public JavaStreamingContext call() throws Exception {
                final String brokers = serverProps.getProperty("kafka.metadata.broker.list");
                final String topics = serverProps.getProperty("kafka.topic");
                final String checkpointPath = serverProps.getProperty("streaming.checkpoint.path");
                final String groupId = serverProps.getProperty("kafka.groupId");

                // 组合kafka参数
                final Map<String, String> kafkaParams = new HashMap<>(6);
                kafkaParams.put("metadata.broker.list", brokers);
                kafkaParams.put("group.id", groupId);
                Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));

                // 获取每个分区的消费到的offset位置
                final KafkaCluster kafkaCluster = getKafkaCluster(kafkaParams);
                Map<TopicAndPartition, Long> consumerOffsetsLong = getConsumerOffsets(kafkaCluster, topics);
//                Map<TopicAndPartition, Long> consumerOffsetsLong = getConsumerOffsets(kafkaCluster, groupId, topicsSet);

                printOffset(consumerOffsetsLong);

                // 需要把每个批次的offset保存到此变量
                final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference();

                SparkConf sparkConf = new SparkConf().setAppName("ScreenMessageStreaming");
                Long duration = Long.parseLong(serverProps.getProperty("streaming.interval"));
                JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(duration));
                jssc.checkpoint(checkpointPath);

                JavaInputDStream<String> kafkaMessageDstream = KafkaUtils.createDirectStream(
                        jssc,
                        String.class,
                        String.class,
                        StringDecoder.class,
                        StringDecoder.class,
                        String.class,
                        kafkaParams,
                        consumerOffsetsLong,
                        new Function<MessageAndMetadata<String, String>, String>() {
                            @Override
                            public String call(MessageAndMetadata<String, String> v1) throws Exception {
                                return v1.message();
                            }
                        }
                );

                // 打印offset消息，并保留offsetRange
                JavaDStream<String> messagesTransform = kafkaMessageDstream.transform(new Function<JavaRDD<String>, JavaRDD<String>>() {
                    @Override
                    public JavaRDD<String> call(JavaRDD<String> rdd) throws Exception {
                        OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                        offsetRanges.set(offsets);

                        for (OffsetRange o : offsetRanges.get()) {
                            log.info("rddoffsetRange:======================================");
                            log.info("rddoffsetRange:topic=" + o.topic()
                                    + ",partition=" + o.partition()
                                    + ",fromOffset=" + o.fromOffset()
                                    + ",untilOffset=" + o.untilOffset()
                                    + ",rddpartitions=" + rdd.getNumPartitions()
                                    + ",isempty=" + rdd.isEmpty()
                                    + ",id=" + rdd.id());
                        }

                        return rdd;
                    }
                });

                // 解析报文数据
                // 将每条用户交易转换成键值对，键是我们自定义的key,值是交易金额，并统计交易金额大小
                //	{"userId":1001,"day":"2019-07-27 ","begintime":1564156800000,"endtime":1564157400000,
                //	"data":[{"system":"com.browser1","transactionsum":60000},
                //	{"system":"com.browser3","transactionsum":120000}]}

                // 将kafka中的消息转换成对象并过滤不合法的消息
                JavaDStream kafkaMessageDstreamFilter = messagesTransform.filter(new Function<String, Boolean>() {
                    @Override
                    public Boolean call(String message) throws Exception {
                        try {
                            // 把json字符串转化成我们的对象
                            UserTransactionRequestModel requestModel = JSONUtil.json2Object(message, UserTransactionRequestModel.class);
                            // 不能转成功，那么返回false进行过滤
                            if (requestModel == null ||
                                    requestModel.getUserId() == 0 ||
                                    requestModel.getSingleUserTransactionRequestModelList() == null ||
                                    requestModel.getSingleUserTransactionRequestModelList().size() == 0) {
                                return false;
                            }

                            return true;
                        } catch (Exception e) {
                            log.error("json to UserTransactionRequestModel error", e);
                            return false;
                        }
                    }
                });

                @SuppressWarnings("unchecked")
                // 两个类，它会比较是否相同和大小关系以便排序
                JavaPairDStream<UserHourSystemKey, Long> javaPairDStream = kafkaMessageDstreamFilter.flatMapToPair(new PairFlatMapFunction<String, UserHourSystemKey, Long>() {
                    @Override
                    public Iterator<Tuple2<UserHourSystemKey, Long>> call(String message) throws Exception {
                        List<Tuple2<UserHourSystemKey, Long>> list = new ArrayList();

                        UserTransactionRequestModel requestModel;
                        try {
                            //把json字符串转化成object，并进行使用
                            requestModel = JSONUtil.json2Object(message, UserTransactionRequestModel.class);
                        } catch (Exception e) {
                            log.error("event body is Invalid,message=" + message, e);
                            return list.iterator();
                        }
                        // 如果没内容那么返回一个空的iterator
                        if (requestModel == null) {
                            return list.iterator();
                        }

                        // 提取system 和 transactionsum内容
                        // [{"system":"com.browser1","transactionsum":60000},
                        // {"system":"com.browser3","transactionsum":120000}]}
                        List<SingleUserTransactionRequestModel> singleList = requestModel.getSingleUserTransactionRequestModelList();

                        for (SingleUserTransactionRequestModel singleModel : singleList) {
                            // {"system":"com.browser1","transactionsum":60000}
                            UserHourSystemKey key = new UserHourSystemKey();
                            // 把userid提出来
                            key.setUserId(requestModel.getUserId());
                            // 把开始时间(毫秒值)转为hour表示
                            key.setHour(DateUtils.getDateStringByMillisecond(DateUtils.HOUR_FORMAT, requestModel.getBeginTime()));
                            // 把系统名称提取出来
                            key.setsystemName(singleModel.getSystemName());

                            Tuple2<UserHourSystemKey, Long> t = new Tuple2(key, singleModel.getTransationSum());
                            list.add(t);
                        }

                        return list.iterator();
                    }
                }).reduceByKey(new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long long1, Long long2) throws Exception {
                        return long1 + long2;
                    }
                });

                // 将每个用户的统计时长写入MySQL
                javaPairDStream.foreachRDD(new VoidFunction<JavaPairRDD<UserHourSystemKey, Long>>() {
                    @Override
                    public void call(JavaPairRDD<UserHourSystemKey, Long> rdd) throws Exception {
                        rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<UserHourSystemKey, Long>>>() {
                            @Override
                            public void call(Iterator<Tuple2<UserHourSystemKey, Long>> it) throws Exception {
                                MysqlTransactionAmountService service = new MysqlTransactionAmountService();
                                while (it.hasNext()) {
                                    Tuple2<UserHourSystemKey, Long> t = it.next();
                                    UserHourSystemKey key = t._1();

                                    UserTransactionAmountModel model = new UserTransactionAmountModel();
                                    model.setUserId(MyStringUtil.getFixedLengthStr(String.valueOf(key.getUserId()), 10));
                                    model.setHour(key.getHour());
                                    model.setSystemNameName(key.getsystemName());
                                    model.setAmount(t._2());
                                    service.addTransactionSum(model);
                                }

                            }
                        });
                        // Kafka offset写入到MySQL
                        offsetToMySQL(offsetRanges, topics, groupId);
                    }
                });
                return jssc;
            }
        };
        return createContextFunc;
    }

    /**
     * 每个RDD将消费的offset写入到MySQL中
     *
     * @param offsetRanges
     * @param topic
     * @param groupId
     */
    public static void offsetToMySQL(final AtomicReference<OffsetRange[]> offsetRanges, final String topic, String groupId) {
        ConsumeOffsetModel model = new ConsumeOffsetModel();
        ConsumeOffsetService offsetService = new ConsumeOffsetService();
        for (OffsetRange or : offsetRanges.get()) {
            model.setGroupId(groupId);
            model.setTopic(topic);
            model.setPartition(String.valueOf(or.partition()));
            model.setOffset(String.valueOf(or.untilOffset()));
            try {
                offsetService.insertOffset(model);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 从MySQL中获取kafka每个分区消费到的offset,以便继续消费
     * 只在重启的时会用到，在运行过程中，是不会读取的
     * @param kafkaCluster
     * @param topic
     * @return
     * @throws Exception
     */
    public static Map<TopicAndPartition, Long> getConsumerOffsets(KafkaCluster kafkaCluster, String topic) throws Exception {
        Set<String> topicSet = new HashSet<>();
        topicSet.add(topic);
        scala.collection.immutable.Set<TopicAndPartition> topicAndPartitionSet =
                kafkaCluster.getPartitions(JavaConversions.asScalaSet(topicSet).<String>toSet()).right().get();
        Map<TopicAndPartition, Long> map = new HashMap(6);
        List<ConsumeOffsetModel> modelList;
        ConsumeOffsetService offsetService = new ConsumeOffsetService();
        modelList = offsetService.getList();
        if (modelList.size() == 0) {
            Set<TopicAndPartition> topicAndPartitionJavaSet = JavaConversions.setAsJavaSet(topicAndPartitionSet);
            for (TopicAndPartition topicAndPartition : topicAndPartitionJavaSet) {
                map.put(topicAndPartition, 0L);
            }
        } else {
            for (ConsumeOffsetModel model : modelList) {
                int partition = Integer.parseInt(model.getPartition());
                long offset = Long.valueOf(model.getOffset());
                TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
                map.put(topicAndPartition, offset);
            }
        }
        return map;
    }

    /**
     * 从zk获取kafka每个分区消费到的offset,以便继续消费
     *
     * @param kafkaCluster
     * @param groupId
     * @param topicSet
     * @return
     */
    public static Map<TopicAndPartition, Long> getConsumerOffsets(KafkaCluster kafkaCluster, String groupId, Set<String> topicSet) {
        //把topic set转变为scala的topic set
        scala.collection.mutable.Set<String> mutableTopics = JavaConversions.asScalaSet(topicSet);
        scala.collection.immutable.Set<String> immutableTopics = mutableTopics.toSet();

        //得到有个scala set里面的数据类型TopicAndPartition
        scala.collection.immutable.Set<TopicAndPartition> topicAndPartitionSet2 = kafkaCluster.getPartitions(immutableTopics).right().get();

        // kafka direct stream 初始化时使用的offset数据
        Map<TopicAndPartition, Long> consumerOffsetsLong = new HashMap(6);

        // 没有保存offset时（该group首次消费时）, 各个partition offset 默认为0
        if (kafkaCluster.getConsumerOffsets(groupId, topicAndPartitionSet2).isLeft()) {
            // 转变为java set
            Set<TopicAndPartition> topicAndPartitionSet1 = JavaConversions.setAsJavaSet(topicAndPartitionSet2);
            // 把每个offset都赋值为0，java.util.Map<kafka.common.TopicAndPartition,Long> fromOffsets
            for (TopicAndPartition topicAndPartition : topicAndPartitionSet1) {
                consumerOffsetsLong.put(topicAndPartition, 0L);
            }

        } else {
            // offset已存在, 使用保存的offset
            scala.collection.immutable.Map<TopicAndPartition, Object> consumerOffsetsTemp = kafkaCluster.getConsumerOffsets(groupId, topicAndPartitionSet2).right().get();
            Map<TopicAndPartition, Object> consumerOffsets = JavaConversions.mapAsJavaMap(consumerOffsetsTemp);
            Set<TopicAndPartition> topicAndPartitionSet1 = JavaConversions.setAsJavaSet(topicAndPartitionSet2);

            for (TopicAndPartition topicAndPartition : topicAndPartitionSet1) {
                Long offset = (Long) consumerOffsets.get(topicAndPartition);
                consumerOffsetsLong.put(topicAndPartition, offset);
            }
        }
        return consumerOffsetsLong;
    }


    public static KafkaCluster getKafkaCluster(Map<String, String> kafkaParams) {
        //把java的map类型转换为scala类型
        scala.collection.mutable.Map<String, String> map = JavaConversions.mapAsScalaMap(kafkaParams);
        scala.collection.immutable.Map<String, String> scalaKafkaParam = map.toMap(new Predef.$less$colon$less<Tuple2<String, String>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> apply(Tuple2<String, String> v1) {
                return v1;
            }
        });
        return new KafkaCluster(scalaKafkaParam);
    }

    /**
     * 打印获取的每个分区消费到的位置
     * @param consumerOffsetsLong
     */
    public static void printOffset(Map<TopicAndPartition, Long> consumerOffsetsLong) {
        Iterator<Map.Entry<TopicAndPartition, Long>> it1 = consumerOffsetsLong.entrySet().iterator();
        while (it1.hasNext()) {
            Map.Entry<TopicAndPartition, Long> entry = it1.next();
            TopicAndPartition key = entry.getKey();
            log.info("partition:offset partition=" + key.partition()+",beginOffset=" + entry.getValue());
        }
    }
}
