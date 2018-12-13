package org.infinivision.flink.streaming.async;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Properties;

public class RdbConnect {
    public static void main(String[] args) throws Exception {
        // read properties
        Properties rdbProps = new Properties();
        InputStream inputStream = AsyncJDBC.class.getClassLoader().getResourceAsStream("postgre.properties");
        rdbProps.load(inputStream);

        String url = rdbProps.getProperty("url");
        String userName = rdbProps.getProperty("userName");
        String password = rdbProps.getProperty("password");
        String driver = rdbProps.getProperty("driver");
        // load the driver
        Class.forName(driver);

        Connection connection = DriverManager.getConnection(url, userName, password);

        String tableName = "pg_sink";

        System.out.println("=====index info=====");
        ResultSet resultSet = connection
                .getMetaData()
                .getIndexInfo(null, null, tableName, true, false);


        while (resultSet.next()) {
            System.out.println("index name: " + resultSet.getString("INDEX_NAME"));
            System.out.println("column name: " + resultSet.getString("COLUMN_NAME"));
        }

        System.out.println("=====column info=====");
        ResultSet rs = connection
                .getMetaData()
                .getColumns(null, null, tableName, null);


        while (rs.next()) {
            System.out.println("column name: " + rs.getString("COLUMN_NAME"));
        }
    }
}
