package org.infinivision.flink.streaming.async;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;

import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

public class AsyncJDBC {
    private final static int DEFAULT_VERTX_EVENT_LOOP_POOL_SIZE = 1;

    private final static int DEFAULT_VERTX_WORKER_POOL_SIZE = Runtime.getRuntime().availableProcessors() * 2;

    private final static int DEFAULT_MAX_DB_CONN_POOL_SIZE = DEFAULT_VERTX_EVENT_LOOP_POOL_SIZE + DEFAULT_VERTX_WORKER_POOL_SIZE;

    public static void main(String[] args) throws Exception {
        // read properties
        Properties postgresProp = new Properties();
        InputStream inputStream = AsyncJDBC.class.getClassLoader().getResourceAsStream("postgre.properties");
        postgresProp.load(inputStream);

        JsonObject pgClientConfig = new JsonObject();
        pgClientConfig.put("url", postgresProp.getProperty("url"))
                .put("driver_class", postgresProp.getProperty("driver"))
                .put("max_pool_size", DEFAULT_MAX_DB_CONN_POOL_SIZE)
                .put("user", postgresProp.getProperty("userName"))
                .put("password", postgresProp.getProperty("password"));

        VertxOptions vo = new VertxOptions();
        vo.setEventLoopPoolSize(DEFAULT_VERTX_EVENT_LOOP_POOL_SIZE);
        vo.setWorkerPoolSize(DEFAULT_VERTX_WORKER_POOL_SIZE);
        Vertx vertx = Vertx.vertx(vo);
        JDBCClient jdbcClient = JDBCClient.createNonShared(vertx, pgClientConfig);

        CompletableFuture<ResultSet> future = new CompletableFuture<>();
        jdbcClient.getConnection(
                conn -> {
                    if (conn.failed()) {
                        throw new RuntimeException(conn.cause());
                    }

                    final SQLConnection connection = conn.result();

                    String sql = "SELECT ID,NAME,EMAIL FROM t_customer limit 10";
                    connection.query(sql, rs -> {
                        if (rs.failed()) {
                            throw new RuntimeException(conn.cause());
                        }

                        ResultSet resultSet = rs.result();

//                        try {
//                            Thread.sleep(5000);
//                        } catch (InterruptedException ex) {
//                            throw new RuntimeException(ex);
//                        }
                        System.out.println("complete future");
                        future.complete(resultSet);
                    });

                    // and close the connection
                    connection.close(done -> {
                        if (done.failed()) {
                            throw new RuntimeException(done.cause());
                        } else {
                            System.out.println("Connection was closed");
                        }
                    });
                }
        );


        if (!future.isDone()) {
            System.out.println("future has not been completed.");
            ResultSet resultSet = future.get();
            System.out.println("====output jsonArray===");
            for (JsonArray array : resultSet.getResults()) {
                System.out.println(array.toString());
            }

            System.out.println("====output Rows====");
            for (JsonObject object : resultSet.getRows()) {
                System.out.println(object);
            }
        }


        jdbcClient.close(done -> {
            if (done.failed()) {
                throw new RuntimeException(done.cause());
            } else {
                System.out.println("Vertx JDBC Client closed");
            }
        });

        vertx.close(done -> {
                    if (done.failed()) {
                        throw new RuntimeException(done.cause());
                    } else {
                        System.out.println("Vertx was closed");
                    }
                }
        );
    }

}
