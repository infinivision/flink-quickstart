package org.infinivision.flink.streaming.cep;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class TagCepExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Event startEvent = new Event(40, "c", 1.0);
        Event middleEvent1 = new Event(41, "a", 2.0);
        Event middleEvent2 = new Event(42, "b", 3.0);


        DataStream<Event> input = env.fromElements(
                middleEvent1
        );

        // c a{2,} b
        Pattern<Event, ?> pattern = Pattern.<Event>begin("start").optional()
                .followedBy("middle").where(new SimpleCondition<Event>() {
                    private static final long serialVersionUID = 5726188262756267490L;

                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.getName().equals("a");
                    }
                }).optional()
                .followedBy("end").where(new SimpleCondition<Event>() {
                    private static final long serialVersionUID = 5726188262756267490L;

                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.getPrice() > 1.0;
                    }
                }).optional();

        System.out.println("=====output the patten=====");
        System.out.println(pattern.toString());
        DataStream<String> result = CEP.pattern(input, pattern).flatSelect((p, o) -> {

            System.out.println();
            System.out.println("=====Match Patten Found=====");

            if (p.containsKey("start")) {
                System.out.println("===Start Patten===");
                System.out.println("Events: " + p.get("start"));
                for (Event event : p.get("start")) {
                    o.collect(event.toString());
                }

            }

            if (p.containsKey("middle")) {
                System.out.println("===middle Patten===");
                System.out.println("Events: " + p.get("middle"));
                for (Event event : p.get("middle")) {
                    o.collect(event.toString());
                }
            }

            if (p.containsKey("end")) {
                System.out.println("===Start Patten===");
                System.out.println("Events: " + p.get("end"));
                for (Event event : p.get("end")) {
                    o.collect(event.toString());
                }
            }

        }, Types.STRING);


        List<String> resultList = new ArrayList<>();

        DataStreamUtils.collect(result).forEachRemaining(resultList::add);
        System.out.println();

        System.out.println("====output the result====");

        for (String r : resultList) {
            System.out.println(r);
        }
    }
}
