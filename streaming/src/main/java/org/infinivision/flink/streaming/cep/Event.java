package org.infinivision.flink.streaming.cep;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.util.Objects;

/**
 * Exemplary event for usage in tests of CEP.
 */
public class Event {
    private String name;
    private double price;
    private int id;

    public Event(int id, String name, double price) {
        this.id = id;
        this.name = name;
        this.price = price;
    }

    public double getPrice() {
        return price;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "Event(" + id + ", " + name + ", " + price + ")";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Event) {
            Event other = (Event) obj;

            return name.equals(other.name) && price == other.price && id == other.id;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, price, id);
    }

    public static TypeSerializer<Event> createTypeSerializer() {
        TypeInformation<Event> typeInformation = (TypeInformation<Event>) TypeExtractor.createTypeInfo(Event.class);

        return typeInformation.createSerializer(new ExecutionConfig());
    }
}

