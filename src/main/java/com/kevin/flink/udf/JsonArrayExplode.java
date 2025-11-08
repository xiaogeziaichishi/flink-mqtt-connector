package com.kevin.flink.udf;


import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.time.Instant;

/**
 * 把一条 JSON 数组消息展开为多行。
 */
@FunctionHint(
        output = @DataTypeHint(
                "ROW<Name STRING, VariableId STRING, Value STRING, TimeStamp STRING, Status STRING>"
        )
)
public class JsonArrayExplode extends TableFunction<Row> {

    private static final ObjectMapper M = new ObjectMapper();

    // 必须要有无参构造器
    public JsonArrayExplode() {
    }

    public void eval(String json) throws Exception {
        if (json == null || json.isEmpty()) return;

        JsonNode root = M.readTree(json);
        if (root == null || !root.isArray()) return;

        ArrayNode arr = (ArrayNode) root;
        for (JsonNode e : arr) {
            String name = textOrNull(e, "Name");
            String varId = textOrNull(e, "VariableId");
            String value = textOrNull(e, "Value");
            String ts = textOrNull(e, "TimeStamp");
            String status = textOrNull(e, "Status");

            collect(Row.of(name, varId, value, ts, status));
        }
    }

    private static String textOrNull(JsonNode n, String key) {
        JsonNode v = n.get(key);
        return v == null || v.isNull() ? null : v.asText();
    }

    private static Double numberOrNull(JsonNode n, String key) {
        JsonNode v = n.get(key);
        if (v == null || v.isNull()) return null;
        if (v.isNumber()) return v.asDouble();
        try {
            return Double.valueOf(v.asText());
        } catch (Exception ignore) {
            return null;
        }
    }

    private static Timestamp timestampOrNull(JsonNode n, String key) {
        JsonNode v = n.get(key);
        if (v == null || v.isNull()) return null;
        String s = v.asText();
        try {
            // 期待 ISO-8601（如 2025-11-06T10:00:01.123Z）
            return Timestamp.from(Instant.parse(s));
        } catch (Exception ex) {
            // 其他格式可按需扩展解析
            return null;
        }
    }
}

