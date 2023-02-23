package basic.day03;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 实现数据流的迭代计算
 *
 * 将输入的数据先应用一个计算模型(就是将数据进行处理), 然后将得到的数据
 * 判断是否满足进行迭代的条件, 如果满足就将该数据再次作为输入数据
 * 如果满足输出的条件, 就将结果输出
 */
public class IterateDemo01 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 得到数据流源
        DataStreamSource<String> strs = env.socketTextStream("linux001", 8888);
        DataStream<Long> numbers = strs.map(Long::parseLong);

        //对Nums进行迭代
        IterativeStream<Long> iteration = numbers.iterate();

        // 将可迭代数据流更新数据模型 (根据自定义的MapFunction)
        DataStream<Long> iterationBody = iteration.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("iterate input =>" + value);
                return value-=1;
            }
        });


        //只要满足value > 0的条件，就会形成一个回路，重新的迭代
        DataStream<Long> feedback = iterationBody.filter(new FilterFunction<Long>(){
            @Override
            public boolean filter(Long value) throws Exception {
                return value > 0;
            }
        });

        // 继续迭代的条件(feedback)
        iteration.closeWith(feedback);


        // 跳出迭代, 输出的条件
        DataStream<Long> output = iterationBody.filter(new FilterFunction<Long>(){
            @Override
            public boolean filter(Long value) throws Exception {
                return value <= 0;
            }
        });

        output.print();

        env.execute();
    }

}
