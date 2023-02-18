package day02;


import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * 使用socket source实时输入如下的数据
 * 订单id,商品分类id,订单金额
 * o01,c11,3000
 * 002,c12,1000
 * 003,c11,2000
 * 004,c13,3000
 * 维度数据在MySQL的一张表中c11,手机
 * c12,家具c13,厨具c14,电脑
 * 使用flink实时读取Source中的数据，然后关联mysql中的维度数据，得到如下的结果o01,c11,3000,手机
 */
public class Execrise01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("linux001", 8888);

        SingleOutputStreamOperator<String> mapped = source.map(new MappingInformation());

        mapped.print();

        env.execute();
    }



    public static class MappingInformation extends RichMapFunction<String, String> {
        Connection conn = null;
        PreparedStatement ps = null;


        // sink后, 获得mysql连接, 并得到相应的查询语句模版
        @Override
        public void open(Configuration parameters) throws Exception {

            // 初始化创建连接 和查询语句
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3307/niuke", "root", "123456");

            ps = conn.prepareStatement("select name from category_tb where id = ?");

        }

        @Override
        public String map(String line) throws Exception {
            String[] words = line.split(",");
            String id = words[0];
            String cid = words[1];
            String money = words[2];
            String category = "null";


            // 设置参数
            ps.setString(1, cid);
            ResultSet rs = ps.executeQuery();

            // 遍历结果
            while(rs.next()) {
                category = rs.getString(1);
            }

            return id + "," +cid + "," + money + "," + category;
        }

        @Override
        public void close() throws Exception {
            // 释放连接
            if(conn != null) {
                conn.close();
            }
        }
    }



}
