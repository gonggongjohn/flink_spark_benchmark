package gonggongjohn.benchmark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import scala.Option;
import scala.Tuple2;
import scala.runtime.AbstractFunction4;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class SparkUVRecorder {
    public static void main(String[] args) throws Exception {
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        //String host = "127.0.0.1";
        //int port = 8082;
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SparkUVRecorder");
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(5));
        streamingContext.sparkContext().setCheckpointDir("spark_check/");
        streamingContext.sparkContext().setLogLevel("ERROR");
        JavaReceiverInputDStream<String> stream = streamingContext.socketTextStream(host, port);
        JavaPairDStream<String, Long> streamTuple = stream.mapToPair(new PairFunction<String, String, Long>() {
            @Override
            public Tuple2<String, Long> call(String s) throws Exception {
                String[] inputList = s.split(",");
                return Tuple2.apply(inputList[0], Long.parseLong(inputList[1]));
            }
        });
        JavaMapWithStateDStream<String, Long, Boolean, Tuple2<String, Boolean>> deduplicatedStream = streamTuple.mapWithState(StateSpec.function(new Function3<String, Optional<Long>, State<Boolean>, Tuple2<String, Boolean>>() {
            @Override
            public Tuple2<String, Boolean> call(String key, Optional<Long> value, State<Boolean> state) throws Exception {
                if(state.exists()) return Tuple2.apply(key + "_" + value.get(), false);
                else{
                    state.update(true);
                    return Tuple2.apply(key + "_" + value.get(), true);
                }
            }
        }));
        JavaDStream<String> resultStream = deduplicatedStream.map(new Function<Tuple2<String, Boolean>, String>() {
            @Override
            public String call(Tuple2<String, Boolean> in) throws Exception {
                String tag;
                if(in._2){
                    tag = "new";
                }
                else{
                    tag = "duplicate";
                }
                return in._1 + " " + tag + " " + System.currentTimeMillis();
            }
        });
        resultStream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> rdd) throws Exception {
                rdd.saveAsTextFile("spark_uv_output_" + System.currentTimeMillis());
            }
        });
        streamingContext.start();
        try {
            streamingContext.awaitTermination();
        } catch (Exception e){
            e.printStackTrace();
        }
        streamingContext.close();
    }
}
