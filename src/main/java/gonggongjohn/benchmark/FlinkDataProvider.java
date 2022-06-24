package gonggongjohn.benchmark;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.FileReader;

public class FlinkDataProvider implements SourceFunction<String> {
    private String path;
    private boolean canceled = false;

    public FlinkDataProvider(String path){
        this.path = path;
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(path));
        while(!canceled && reader.ready()){
            String line = reader.readLine();
            sourceContext.collect(line);
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        this.canceled = true;
    }
}
