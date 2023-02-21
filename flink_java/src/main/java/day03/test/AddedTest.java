package day03.test;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class AddedTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 将数据读取
        DataStreamSource<String> source = env.socketTextStream("linux001", 8889);

        SingleOutputStreamOperator<String> mapToAddedMap = source.transform("mapToAddedMap", TypeInformation.of(String.class), new MapperStream());

//        mapToAddedMap.setParallelism(12);

        mapToAddedMap.print();

        // 使用转换算子, 读取数据的时候, 使用map方法, 将中间结果进行存储
        //  得到输入, 通过从map的取值,,每次的输出, 都是map的集合转换的字符串


        env.execute();
    }


    public static class MapperStream extends AbstractStreamOperator<String> implements OneInputStreamOperator<String, String> {

        static Map<String, Integer> provinceMap = null;
        static Map<String, Integer> cityMap = null;

        public MapperStream() {
            provinceMap = new ConcurrentHashMap<>();
            cityMap = new ConcurrentHashMap<>();
        }



        @Override
        public void processElement(StreamRecord<String> element) throws Exception {
            String line = element.getValue();
            String[] info = line.split(",");


            String province = info[0];
            String city = info[1];
            int money = Integer.parseInt(info[2]);


            StringBuilder tt = new StringBuilder();


            int provinceMoneyInMap = provinceMap.getOrDefault(province, 0);
            int cityMoneyInMap = cityMap.getOrDefault(city, 0);


            provinceMap.put(province ,provinceMoneyInMap  + money);
            cityMap.put(city, cityMoneyInMap + money);

            int i = 0;
            for (String pro : provinceMap.keySet()) {
                if(i % 3 == 0) {
                    tt.append("\n");
                }
                tt.append(" ").append(pro).append(",").append(provinceMap.get(pro)) ;
                i++;
            }

            for (String ci : cityMap.keySet()) {
                if(i % 3 == 0) {
                    tt.append("\n");
                }
                tt.append(" ").append(ci).append(",").append(cityMap.get(ci));
                i++;
            }

            element.replace(tt.toString());
            output.collect(element);

        }
    }
}
