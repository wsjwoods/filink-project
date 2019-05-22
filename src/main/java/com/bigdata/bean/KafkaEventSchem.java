package com.bigdata.bean;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class KafkaEventSchem implements DeserializationSchema<KafkaEvent>, SerializationSchema<KafkaEvent> {

    @Override
    public KafkaEvent deserialize(byte[] message) throws IOException {
        return KafkaEvent.fromString(new String(message));
    }

    @Override
    public boolean isEndOfStream(KafkaEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<KafkaEvent> getProducedType() {
        return TypeInformation.of(KafkaEvent.class);
    }

    @Override
    public byte[] serialize(KafkaEvent element) {
        return element.toString().getBytes();
    }
}
