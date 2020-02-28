package org.apache.flink.playgrounds.ops.clickcount;

import com.alibaba.fastjson.JSONObject;
import org.nn.flink.streaming.connectors.kudu.TableSerializationSchema;

public class MyTableSerializationSchema implements TableSerializationSchema<JSONObject> {

    private static final long serialVersionUID = 1L;
    private String name;
    public MyTableSerializationSchema(String name) {
        this.name = name;
    }

    @Override
    public String serializeTableName(JSONObject value) {
        return name;
    }
}
