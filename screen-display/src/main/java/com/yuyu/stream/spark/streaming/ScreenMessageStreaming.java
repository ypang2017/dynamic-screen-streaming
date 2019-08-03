package com.yuyu.stream.spark.streaming;

import com.yuyu.stream.common.utils.DateUtils;
import com.yuyu.stream.common.utils.JSONUtil;
import com.yuyu.stream.common.utils.MyStringUtil;
import com.yuyu.stream.spark.key.UserHourSystemKey;
import com.yuyu.stream.spark.model.SingleUserTransactionRequestModel;
import com.yuyu.stream.spark.model.UserTransactionAmountModel;
import com.yuyu.stream.spark.model.UserTransactionRequestModel;
import com.yuyu.stream.spark.service.impl.MysqlTransactionAmountService;
import kafka.serializer.StringDecoder;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author yuyu
 */
public class ScreenMessageStreaming {
    public static final Logger log = Logger.getLogger(ScreenMessageStreaming.class);
    private AtomicReference<OffsetRange[]> offsetRanges;

    public static void main(String[] args) throws InterruptedException {
//        if (args.length < 1) {
//            System.err.println("Usage: ScreenMessageStreaming ConfigFile");
//            System.exit(1);
//        }

        String[] args0 = {
                "yuyu01:9092,yuyu02:9092,yuyu03:9092",
                "dynamic-screen"
        };

        String brokers = args0[0];
        String topics = args0[1];

        // 创建2s一个批次间隔的context
        SparkConf sparkConf = new SparkConf().setAppName("ScreenMessageStreaming");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));
        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers);

        // 通过brokers和topics创建direct kafka stream
        JavaPairInputDStream javaPairInputDStream = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topicsSet
        );

        @SuppressWarnings("unchecked")
        JavaDStream<String> messages = javaPairInputDStream.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tuple2) {
                return tuple2._2();
            }
        });

        // 解析报文数据
        // 将每条用户交易转换成键值对，键是我们自定义的key,值是交易金额，并统计交易金额大小
        //	{"userId":1001,"day":"2019-07-27 ","begintime":1564156800000,"endtime":1564157400000,
        //	"data":[{"system":"com.browser1","transactionsum":60000},
        //	{"system":"com.browser3","transactionsum":120000}]}

        // 将kafka中的消息转换成对象并过滤不合法的消息
        JavaDStream kafkaMessageMessageDstreamFilter = messages.filter(new Function<String, Boolean>() {
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
        //两个类，它会比较是否相同和大小关系以便排序
        JavaPairDStream<UserHourSystemKey, Long> javaPairDStream = kafkaMessageMessageDstreamFilter.flatMapToPair(new PairFlatMapFunction<String, UserHourSystemKey, Long>() {
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
                //如果没内容那么返回一个空的iterator
                if (requestModel == null) {
                    return list.iterator();
                }

                // 提取system 和 transactionsum内容
                // [{"system":"com.browser1","transactionsum":60000},
                // {"system":"com.browser3","transactionsum":120000}]}
                List<SingleUserTransactionRequestModel> singleList = requestModel.getSingleUserTransactionRequestModelList();

                for (SingleUserTransactionRequestModel singleModel : singleList) {
                    //{"package":"com.browser1","activetime":60000}
                    UserHourSystemKey key = new UserHourSystemKey();
                    //把userid提出来
                    key.setUserId(requestModel.getUserId());
                    //把开始时间(毫秒值)转为hour表示
                    key.setHour(DateUtils.getDateStringByMillisecond(DateUtils.HOUR_FORMAT, requestModel.getBeginTime()));
                    //把系统名称提取出来
                    key.setsystemName(singleModel.getSystemName());

                    Tuple2<UserHourSystemKey, Long> t = new Tuple2(key, singleModel.getTransationSum() / 1000);
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

        //将每个用户的统计时长写入MySQL
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
            }
        });

        // 开始执行
        jssc.start();
        jssc.awaitTermination();

    }
}
