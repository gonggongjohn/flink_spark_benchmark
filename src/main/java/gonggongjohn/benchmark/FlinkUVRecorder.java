package gonggongjohn.benchmark;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import java.util.concurrent.TimeUnit;

public class FlinkUVRecorder {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        DataStream<String> stream = env.socketTextStream("127.0.0.1", 8082);
        //DataStream<String> stream = env.addSource(new FlinkDataProvider("source.txt"));
        SingleOutputStreamOperator<String> streamPair = stream.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                String[] split = s.split(",");
                Tuple2<String, Long> res = Tuple2.apply(split[0], Long.parseLong(split[1]));
                collector.collect(res);
            }
        }).keyBy(s -> s._1).process(new Deduplicator());
        streamPair.writeAsText("flink_uv_output_" + System.currentTimeMillis() + ".txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        //streamPair.print();
        JobExecutionResult result = env.execute("Flink UV Recorder");
    }

    public static class Deduplicator extends KeyedProcessFunction<String, Tuple2<String, Long>, String>{
        private ValueState<String> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("uvstate", String.class);
            stateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(60)).build());
            state = getRuntimeContext().getState(stateDescriptor);
        }

        @Override
        public void processElement(Tuple2<String, Long> in, KeyedProcessFunction<String, Tuple2<String, Long>, String>.Context context, Collector<String> collector) throws Exception {
            String cur = state.value();
            if(cur == null){
                cur = in._1;
                state.update(cur);
                collector.collect(in._1 + "_" + in._2 + " new " + System.currentTimeMillis());
            }
            else{
                collector.collect(in._1 + "_" + in._2 + " duplicate " + System.currentTimeMillis());
            }
        }
    }
}
