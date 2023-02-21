package day04;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class JDBCSinkDemo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // o1,3000,手机
        // o2,400,化妆品
        DataStreamSource<String> source = env.socketTextStream("linux001", 8888);

        SingleOutputStreamOperator<Tuple3<String, Double, String>> tp = source.map(line -> {
            String[] arr = line.split(",");

            return Tuple3.of(arr[0], Double.parseDouble(arr[1]), arr[2]);
        }).returns(Types.TUPLE(Types.STRING,Types.DOUBLE,Types.STRING));

        SinkFunction<Tuple3<String, Double, String>> sink = JdbcSink.sink("insert into order_tb values( ? ,?,?)",
                new JdbcStatementBuilder<Tuple3<String, Double, String>>() {
                    // 将数据和ps进行参数绑定
                    @Override
                    public void accept(PreparedStatement preparedStatement, Tuple3<String, Double, String> tp) throws SQLException {
                        preparedStatement.setString(1, tp.f0);
                        preparedStatement.setDouble(2, tp.f1);
                        preparedStatement.setString(3, tp.f2);

                    }
                },
                JdbcExecutionOptions.builder() //执行相关的参数
                        .withBatchSize(100)
                        .withBatchIntervalMs(100)
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder() // 链接相关的参数
                        .withUrl("jdbc:mysql://localhost:3307/niuke")
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("123456")
                        .build()

        );

        tp.addSink(sink);

        // 如果主键重复, 两种解决方案
        // 1.追加
        // 2.更新

        env.execute();



    }
}
