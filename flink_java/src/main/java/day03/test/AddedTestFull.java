package day03.test;


import com.mysql.cj.xdevapi.Type;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 使用分布式计算, 利用多并行的方式
 */
public class AddedTestFull {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 将数据读取
        DataStreamSource<String> source = env.socketTextStream("linux001", 8888);


        SingleOutputStreamOperator<List<Map<String, Integer>>> gomap = source.transform("mapToAddedMap", TypeInformation.of(new TypeHint<List<Map<String, Integer>>>() {
        }), new MultiMapper());

//        gomap.keyBy(10).var

        // 使用转换算子, 读取数据的时候, 使用map方法, 将中间结果进行存储
        // 在RichMapFunction中, 得到输出, 每次的输出, 都是map的集合转换成的string,


        env.execute();
    }

    public static class MultiMapper extends AbstractStreamOperator<List<Map<String, Integer>>> implements OneInputStreamOperator<String, List<Map<String, Integer>>> {

        Map<String, Integer> provinceMap = null;
        Map<String, Integer> cityMap = null;

        public MultiMapper() {
            provinceMap = new HashMap<>();
            cityMap = new HashMap<>();
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
            ArrayList<Map<String, Integer>> list = new ArrayList<>();


            list.add(cityMap);
            list.add(provinceMap);

            output.collect(new StreamRecord<>(list));

        }
    }
}
