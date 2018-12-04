package org.infinivision.flink.streaming.cep;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.io.InputStream;
import java.sql.Date;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class EventTagCepExample {
    public static void main(String[] args) throws Exception {
        InputStream inputStream = EventTagCepExample.class.getClassLoader().getResourceAsStream("kafka.properties");

        Properties kafkaProps = new Properties();
        kafkaProps.load(inputStream);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();

        // set restart strategy
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));

        // config checkpoint settings
        env.enableCheckpointing(60000);

        System.out.println("topic: " + kafkaProps.getProperty("input-topic"));
        FlinkKafkaConsumer011<EventTag> kafkaConsumer011 = new FlinkKafkaConsumer011<>(
                kafkaProps.getProperty("input-topic"),
                new EventTagSchema(),
                kafkaProps);
        kafkaConsumer011.setStartFromEarliest();

        DataStream<EventTag> input = env.addSource(kafkaConsumer011);

        AfterMatchSkipStrategy matchSkipStrategy = AfterMatchSkipStrategy.skipPastLastEvent();
        // construct the cep pattern
        Pattern<EventTag, ?> pattern = Pattern.<EventTag>begin("start", matchSkipStrategy).optional()
                .followedBy("gender").where(new SimpleCondition<EventTag>() {
                    @Override
                    public boolean filter(EventTag value) throws Exception {
                        return value.getGender().equals("女");
                    }
                }).optional()
                .followedBy("language").where(new SimpleCondition<EventTag>() {
                    @Override
                    public boolean filter(EventTag value) throws Exception {
                        return value.getLanguage().toLowerCase().equals("en_us");
                    }
                }).optional()
                .followedBy("birthday").where(new SimpleCondition<EventTag>() {
                    Date begin = Date.valueOf("1980-01-01");
                    Date end = Date.valueOf("1989-12-31");

                    @Override
                    public boolean filter(EventTag value) throws Exception {
                        Date birthday = Date.valueOf(value.getBirthday());
                        return birthday.after(begin) && birthday.before(end);
                    }
                }).optional()
                .followedBy("city").where(new SimpleCondition<EventTag>() {
                    @Override
                    public boolean filter(EventTag value) throws Exception {
                        return value.getCity().toLowerCase().equals("memphis");
                    }
                }).optional();

        System.out.println("=====output the patten=====");
        System.out.println(pattern.toString());
        DataStream<Tuple2<Integer, String>> result = CEP.pattern(input, pattern).flatSelect((p, o) -> {

            System.out.println();
            System.out.println("===Pattern Matched====");
            if (p.containsKey("gender")) {
                for (EventTag event : p.get("gender")) {
                    System.out.println("Events: " + Tuple2.of(event.getId(), "gender"));

                    o.collect(Tuple2.of(event.getId(), "gender"));
                }

            }

            if (p.containsKey("language")) {
                for (EventTag event : p.get("language")) {
                    System.out.println("Events: " + Tuple2.of(event.getId(), "language"));
                    o.collect(Tuple2.of(event.getId(), "language"));
                }
            }

            if (p.containsKey("birthday")) {
                for (EventTag event : p.get("birthday")) {
                    System.out.println("Events: " + Tuple2.of(event.getId(), "birthday"));
                    o.collect(Tuple2.of(event.getId(), "birthday"));
                }
            }

            if (p.containsKey("city")) {
                for (EventTag event : p.get("city")) {
                    System.out.println("Events: " + Tuple2.of(event.getId(), "city"));
                    o.collect(Tuple2.of(event.getId(), "city"));
                }
            }

        }, Types.TUPLE(Types.INT, Types.STRING));


        List<Tuple2<Integer, String>> resultList = new ArrayList<>();

        DataStreamUtils.collect(result).forEachRemaining(resultList::add);
        System.out.println();

        System.out.println("====output the result====");

        for (Tuple2<Integer, String> r : resultList) {
            System.out.println(r);
        }

    }
}
