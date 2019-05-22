package com.bigdata.tableAndsql;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.io.IOException;


public class KafkaCsvSourceSchem implements DeserializationSchema<Row> {

    private TypeInformation<Row> typeInfo;

    private String[] names;

    private TypeInformation<?>[] types;

    public KafkaCsvSourceSchem(TypeInformation<Row> typeInfo) {
        this.typeInfo = typeInfo;
        this.names = ((RowTypeInfo)typeInfo).getFieldNames();
        this.types = ((RowTypeInfo)typeInfo).getFieldTypes();
    }

    @Override
    public Row deserialize(byte[] message) throws IOException {
        String strMessage = new String(message);
        String[] split = strMessage.split(",");

        //这里可能存在一个问题，就是message  split之后的长度和自己定义的names的长度不匹配
        Row row = new Row(names.length);
        for (int i = 0; i < names.length; i++) {
            row.setField(i,split[i]);
        }

        return row;
    }

    @Override
    public boolean isEndOfStream(Row nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return typeInfo;
    }
}
