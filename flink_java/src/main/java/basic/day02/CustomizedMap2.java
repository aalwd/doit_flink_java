package basic.day02;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * 自定义map方法, 使用更加底层的方式,
 * 实现map方法中, transform方法中的参数, 类似与StreamMap类所相似的功能
 *
 */

public class CustomizedMap2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("linux001", 8888);


        MapFunction<String, String> mapFunction = new MapFunction<String, String>() {

            @Override
            public String map(String value) throws Exception {
                return value.toUpperCase();
            }
        };

        // 名称, 输出类型, 以及操作的实现类
        // TypeInformation 类 当类型中有泛型的时候, 会帮助抽取泛型类
        SingleOutputStreamOperator<String> mapped = source.transform("map", TypeInformation.of(String.class), new MyStreamMap(mapFunction));


        mapped.print();


        env.execute();


    }


    public static class MyStreamMap extends AbstractStreamOperator<String> implements OneInputStreamOperator<String, String> {
        private MapFunction<String,String> mapFunction = null;

        public MyStreamMap(MapFunction<String, String> mapFunction) {
            this.mapFunction = mapFunction;
        }



        @Override
        public void processElement(StreamRecord<String> element) throws Exception {
            // 取出值,
            String value = element.getValue();

            // 使用甲方逻辑执行
            String out = mapFunction.map(value);

            // 将原来的容器中的元素, 替换成新生成的结果
            // 并使用output对象收集起来
            output.collect(element.replace(out));

        }
    }


}
