package org.infinivision.flink.streaming.tablesql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaTableSourceTest {

    public static void main(String[] args) throws Exception {
        Map<String, TypeInformation<?>> fields = new HashMap<>();

        fields.put("id", Types.INT);
        fields.put("name", Types.STRING);
//        fields.put("email", Types.STRING);
//        fields.put("phoneNum", Types.STRING);
//        fields.put("gender", Types.STRING);
//        fields.put("age", Types.INT);
//        fields.put("education", Types.STRING);
//        fields.put("birthday", Types.SQL_DATE);
//        fields.put("cardId", Types.STRING);
//        fields.put("monthlySalary", Types.INT);
//        fields.put("yearSalary", Types.INT);
//        fields.put("language", Types.STRING);
//        fields.put("state", Types.STRING);
//        fields.put("province", Types.STRING);
//        fields.put("city", Types.STRING);
//        fields.put("company", Types.STRING);
//        fields.put("position", Types.STRING);
//        fields.put("companyAddress", Types.STRING);
//        fields.put("companyDistrict", Types.STRING);
//        fields.put("resideAddress", Types.STRING);
//        fields.put("resideDistrict", Types.STRING);
//        fields.put("registerDate", Types.LONG);
//        fields.put("registerChannel", Types.STRING);
        fields.put("updateTime", Types.SQL_DATE);
        fields.put("rowTime", Types.SQL_TIMESTAMP);
//        fields.put("platformId", Types.INT);
//        fields.put("platformName", Types.STRING);

        final StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        final StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(streamEnv);
        InputStream inputStream = KafkaTableSourceTest.class.getClassLoader().getResourceAsStream("kafka.properties");
        Properties kafkaProps = new Properties();
        kafkaProps.load(inputStream);

        // init connect
        ConnectorDescriptor kafka = new Kafka()
                .version(kafkaProps.getProperty("version", "0.11"))
                .topic(kafkaProps.getProperty("input-topic", "test"))
                .startFromEarliest()
                .properties(kafkaProps);

        // init format
        Json jsonFormat = new Json();

        // derive from schema
//        jsonFormat.deriveSchema()
//                .failOnMissingField(true);

        TypeInformation[] fieldTypes = {
                Types.INT,
                Types.STRING,
                Types.SQL_DATE,
                Types.LONG
        };


        String[] fieldNames = {"id", "name", "updateTime", "createTime"};
        RowTypeInfo returnType = new RowTypeInfo(fieldTypes, fieldNames);

        jsonFormat.schema(returnType);

        Rowtime rowtime = new Rowtime();
        rowtime.timestampsFromField("createTime")
                .watermarksPeriodicAscending();

        // init schema
        // the RowTime fields must be different with return Type
        Schema schema = new Schema();
        for (String key : fields.keySet()) {
            schema.field(key, fields.get(key));
            if (key.equals("rowTime")) {
                schema.rowtime(rowtime);
            }
        }

        // register Table Source
        StreamTableDescriptor tableDescriptor = tableEnv.connect(kafka)
                .withFormat(jsonFormat)
                .withSchema(schema)
                .inAppendMode();

        tableDescriptor.registerTableSource("kafkaTable");

        Table table = tableEnv.scan("kafkaTable")
                .select("id, name, rowTime, updateTime")
                .where("cast(rowTime, long) > 1483200000");

        DataStream<Row> output = tableEnv.toAppendStream(table, Types.ROW(Types.INT, Types.STRING, Types.SQL_TIMESTAMP, Types.SQL_DATE));
        output.print();

        streamEnv.execute("kafka table");

    }
}
