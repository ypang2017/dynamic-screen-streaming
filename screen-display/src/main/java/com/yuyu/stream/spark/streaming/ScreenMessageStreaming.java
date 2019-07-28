package com.yuyu.stream.spark.streaming;

import com.yuyu.stream.spark.model.SingleUserTransactionRequestModel;
import com.yuyu.stream.spark.model.UserTransactionRequestModel;
import com.yuyu.stream.common.utils.DateUtils;
import com.yuyu.stream.common.utils.JSONUtil;
import com.yuyu.stream.spark.key.UserHourSystemKey;
import kafka.serializer.StringDecoder;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
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
        JavaPairInputDStream messages = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topicsSet
        );



        @SuppressWarnings("unchecked")
        JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tuple2) {
                return tuple2._2();
            }
        });
        lines.print();

        // 开始打印获取到的报文内容
        jssc.start();
        jssc.awaitTermination();
    }
}
