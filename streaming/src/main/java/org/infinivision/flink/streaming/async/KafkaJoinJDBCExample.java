package org.infinivision.flink.streaming.async;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

public class KafkaJoinJDBCExample {
    public static void main(String[] args) throws Exception {
        // set up the execution environment

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // the host and the port to connect to
        final String host;
        final int port;
        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            host = params.get("host", "172.19.0.15");
            port = params.getInt("port", 9999);

            // make parameters available in the web interface

            env.getConfig().setGlobalJobParameters(params);

        } catch (Exception e) {
            System.err.println("No port specified. Please run 'SocketWindowWordCount " +
                    "--hostname <hostname> --port <port>', where hostname (localhost by default) " +
                    "and port is the address of the text server");
            System.err.println("To start a simple text server, run 'netcat -l <port>' and " +
                    "type the input text into the command line");
            return;
        }

        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        DataStream<String> text = env.socketTextStream(host, port, "\n");

        text.flatMap(new JoinMySQLRecord()).print();

        env.execute("Kafka Join MySQL Example");

    }

    private final static class JoinMySQLRecord extends RichFlatMapFunction<String, TrainRecord> {
        private Connection connection;
        private PreparedStatement ps;
        boolean isRunning = true;
        private LoadingCache<String, Collection<TrainRecord>> cacheRecords;
//        final InputStream inputStream = JoinMySQLRecord.class.getClassLoader()
//                .getResourceAsStream("mysql.properties");


        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            // get system parameter from parameter tool
//            final ParameterTool params = ParameterTool.fromPropertiesFile(inputStream);
//            String driver = params.get("driver", "");
//            String url = params.get("url", "");
//            String userName = params.get("userName", "");
//            String password = params.get("password", "");

            // initialize the mysql
            String driver = "com.mysql.jdbc.Driver";
            String url = "jdbc:mysql://172.19.0.15:13306/flink_source?characterEncoding=utf8&useSSL=true";
            String userName = "root";
            String password = "flink";

            // load the driver
            Class.forName(driver);

            // create the connection
            connection = DriverManager.getConnection(url, userName, password);


            cacheRecords = CacheBuilder.newBuilder()
                    .maximumSize(1000000)
                    .expireAfterAccess(7, TimeUnit.DAYS)
                    .build(new CacheLoader<String, Collection<TrainRecord>>() {
                        @Override
                        public Collection<TrainRecord> load(String key) throws Exception {
                            return loadFromMysql(key);
                        }
                    });
        }

        @Override
        public void flatMap(String aid, Collector<TrainRecord> out) throws Exception {
            // get from cache
            Collection<TrainRecord> records = cacheRecords.get(aid);
            for (TrainRecord record : records) {
                out.collect(record);
            }

        }

        @Override
        public void close() throws Exception {
            super.close();

            // close the mysql
            if (ps != null) {
                ps.close();
            }

            if (connection != null) {
                connection.close();
            }
        }

        private Collection<TrainRecord> loadFromMysql(final String aid) throws Exception {
            // construct the SQL
            System.out.println("====Loading From MySQL====");
            String sql = String.format("select * from train where aid = %d limit 10", Integer.valueOf(aid));
            System.out.println("flatMap SQL: " + sql);
            ps = connection.prepareStatement(sql);
            ResultSet resultSet = ps.executeQuery();
            ArrayList<TrainRecord> records = new ArrayList<>();
            while (resultSet.next()) {
                int uid = resultSet.getInt("uid");
                int label = resultSet.getInt("label");
                records.add(new TrainRecord(Integer.valueOf(aid), uid, label));
            }

            if (ps != null) {
                ps.close();
            }
            return records;
        }
    }


}
