package day02.udfs;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class QueryCategoryNameFromDB extends RichMapFunction<String, String> {
    Connection conn = null;
    PreparedStatement ps = null;

    /// 静态变量， 让多线程进行共享
    static Map<String, String> map = new ConcurrentHashMap<>();

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

        if(map.containsKey(cid)) {
            category = map.get(cid);
        } else  {
            // 遍历结果
            while (rs.next()) {
                category = rs.getString(1);
            }
            rs.close();
            map.put(cid, category);
        }

        return id + "," + cid + "," + money + "," + category;
    }

    @Override
    public void close() throws Exception {
        // 释放连接
        if (ps != null) {
            ps.close();
        }
        if (conn != null) {
            conn.close();
        }
    }
}

