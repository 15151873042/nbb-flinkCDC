import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Test {


    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
//        env.setParallelism(3);

        FileSource<String> source = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("input/word.txt")).build();

        DataStreamSource<String> fileSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "fileSource");

        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMapStream = fileSource
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>> (){
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] words = value.split(" ");
                        for (String word : words) {
                            out.collect(Tuple2.of(word, 1));
                        }
                    }
                });

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = flatMapStream
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                });

        SingleOutputStreamOperator<Tuple2<String, Integer>> sumStream = keyedStream.sum(1);
        sumStream.print();

//        SingleOutputStreamOperator<Tuple2<String, Integer>> reduceStream = keyedStream
//                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
//                    @Override
//                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
//                        Integer count = value1.f1 + value2.f1;
//                        return Tuple2.of(value1.f0, count);
//                    }
//                });
//        reduceStream.print();

        env.execute();

    }

}
