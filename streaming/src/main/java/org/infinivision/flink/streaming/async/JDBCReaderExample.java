package org.infinivision.flink.streaming.async;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.io.InputStream;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class JDBCReaderExample {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();

        InputStream inputStream = JDBCReaderExample.class.getClassLoader()
                .getResourceAsStream("mysql.properties");
        final ParameterTool params = ParameterTool.fromPropertiesFile(inputStream);

        String driver = params.get("driver", "");
        String url = params.get("url", "");
        String userName = params.get("userName", "");
        String password = params.get("password", "");
        StringBuilder configStringBuilder = new StringBuilder();

        final String lineSeparator = System.getProperty("line.separator");

        configStringBuilder
                .append("Job configuration").append(lineSeparator)
                .append("mysql driver: ").append(driver).append(lineSeparator)
                .append("mysql url: ").append(url).append(lineSeparator)
                .append("mysql username: ").append(userName).append(lineSeparator)
                .append("mysql password: ").append(password);

        System.out.println(configStringBuilder.toString());

        env.getConfig().setGlobalJobParameters(params);

        DataStream<TrainRecord> input = env.addSource(new JdbcReader(),
                "mysql-reader");

        input.print();

        env.execute("mysql JDBC Source Example");
    }

    public static class JdbcReader extends RichSourceFunction<TrainRecord> {
        private Connection connection;
        private PreparedStatement ps;
        boolean isRunning = true;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);


            Map<String, String> globalParameters = getRuntimeContext().getExecutionConfig()
                    .getGlobalJobParameters().toMap();

            String mysqlDriver = globalParameters.getOrDefault("driver", "NULL");
            String mysqlUrl = globalParameters.getOrDefault("url", "NULL");
            String mysqlUserName = globalParameters.getOrDefault("userName", "NULL");
            String mysqlPassword = globalParameters.getOrDefault("password", "NULL");

            // load the driver
            Class.forName(mysqlDriver);

            // create the connection
            connection = DriverManager.getConnection(mysqlUrl, mysqlUserName, mysqlPassword);

            // sql
            String sql = "select * from train limit 100;";
            ps = connection.prepareStatement(sql);
        }

        @Override
        public void run(SourceContext<TrainRecord> ctx) {
            try {
                ResultSet resultSet = ps.executeQuery();
                while (resultSet.next() && isRunning) {
                    int aid = resultSet.getInt("aid");
                    int uid = resultSet.getInt("uid");
                    int label = resultSet.getInt("label");
                    ctx.collect(new TrainRecord(aid, uid, label));
                }

                // all the data have been loaded
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }

        @Override
        public void close() throws Exception {
            super.close();
            if (ps != null) {
                ps.close();
            }

            if (connection != null) {
                connection.close();
            }
        }
    }

}
