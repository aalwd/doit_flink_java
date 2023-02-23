package basic.day02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

import java.lang.reflect.Array;

public class CustomizedFlatMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("linux001", 8888);

        FlatMapFunction<String, String> flatMapFunction = new FlatMapFunction<String, String>() {

            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {

                for (String s : value.split("\\s+")) {
                    out.collect(s);
                }

            }
        };

        SingleOutputStreamOperator<String> myFlatMap = source.transform("MyFlatMap", TypeInformation.of(String.class), new MyStreamFlatMap(flatMapFunction));

        myFlatMap.print();

        env.execute();
    }



    public static class MyStreamFlatMap extends AbstractStreamOperator<String> implements OneInputStreamOperator<String, String> {

        private FlatMapFunction<String, String> flatMapFunction = null ;

        private TimestampedCollector<String> collector = null;

        public MyStreamFlatMap(FlatMapFunction<String, String> flatMapFunction) {
            this.flatMapFunction = flatMapFunction;

        }

        @Override
        public void open() throws Exception {
            collector = new TimestampedCollector<>(output);
        }

        @Override
        public void processElement(StreamRecord<String> element) throws Exception {

            String value = element.getValue();
            
            collector.setTimestamp(element);

            flatMapFunction.flatMap(value, collector);


        }


    }
}
