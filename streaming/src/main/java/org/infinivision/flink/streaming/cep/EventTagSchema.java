package org.infinivision.flink.streaming.cep;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class EventTagSchema implements DeserializationSchema<EventTag> {
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public EventTag deserialize(byte[] message) throws IOException  {
        return mapper.readValue(message, EventTag.class);
    }

    @Override
    public boolean isEndOfStream(EventTag nextElement) {
        return false;
    }

    @Override
    public TypeInformation<EventTag> getProducedType() {
        return TypeInformation.of(EventTag.class);
    }
}
