import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
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
        env.setParallelism(5);

        FileSource<String> source = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("input/word.txt")).build();

        DataStreamSource<String> fileSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "fileSource");

        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMapSource = fileSource
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>> (){
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] words = value.split(" ");
                        for (String word : words) {
                            out.collect(Tuple2.of(word, 1));
                        }
                    }
                });

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = flatMapSource
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                });

        SingleOutputStreamOperator<Tuple2<String, Integer>> sumStream = keyedStream.sum(1);

        sumStream.print();

        env.execute();

//        SingleOutputStreamOperator<Tuple2<String, Integer>> aaa = flatMapSource
//                .keyBy(tuple -> tuple._1)
//                .map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
//                    @Override
//                    public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
//                        Integer count = state.get(value._1);
//                        count = count == null ? 1 : count + 1;
//                        state.put(value._1, count);
//                        return new Tuple2<String, Integer>(value._1, count);
//                    }
//
//                    MapState<String, Integer> state;
//
//                    @Override
//                    public void open(OpenContext openContext) throws Exception {
//                        MapStateDescriptor<String, Integer> descriptor = new MapStateDescriptor<>("是否是新用户", String.class, Integer.class);
//                        this.state = getRuntimeContext().getMapState(descriptor);
//                    }
//                });
//
//        aaa.print();

//        env.execute();


    }


}
