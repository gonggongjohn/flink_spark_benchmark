package gonggongjohn.benchmark;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SparkUVRecorder {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SparkUVRecorder");
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(1));
    }
}
