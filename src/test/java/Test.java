import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.calcite.shaded.org.checkerframework.checker.units.qual.K;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import scala.Tuple2;

public class Test {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FileSource<String> source =
             FileSource.forRecordStreamFormat(
                           new TextLineInputFormat(), new Path("d:\1.txt"))
             .build();
        DataStreamSource<String> fileSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "fileSource");

        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMapSource = fileSource.flatMap(new MyFlatMap());

        SingleOutputStreamOperator<Tuple2<String, Integer>> aaa = flatMapSource
                .keyBy(tuple -> tuple._1)
                .map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {


                    @Override
                    public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                        Integer count = state.get(value._1);
                        count = count == null ? 1 : count + 1;
                        state.put(value._1, count);
                        return new Tuple2<String, Integer>(value._1, count);
                    }

                    MapState<String, Integer> state;

                    @Override
                    public void open(OpenContext openContext) throws Exception {
                        MapStateDescriptor<String, Integer> descriptor = new MapStateDescriptor<>("是否是新用户", String.class, Integer.class);
                        this.state = getRuntimeContext().getMapState(descriptor);
                    }
                });

        aaa.print();

        env.execute();


    }

    public static class MyFlatMap implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = value.split(",");
            for (String word : words) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }

}
