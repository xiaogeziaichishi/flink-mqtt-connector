package com.kevin.flink.table.connector.mqtt.format;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.util.StringUtils;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Deserializes MQTT payloads that contain a JSON array into Flink {@link RowData}.
 *
 * <p>The table schema must declare exactly one top-level column of ARRAY type.
 * The JSON array's elements are converted to the declared element type, which
 * can be a row (JSON object), another array, or a supported scalar.
 */
public class ArrayJsonRowDataDeserializationSchema implements DeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final RowType rowType;
    private final ArrayType arrayType;
    private final LogicalType elementType;
    private final boolean ignoreParseErrors;

    public ArrayJsonRowDataDeserializationSchema(RowType rowType, boolean ignoreParseErrors) {
        this.rowType = Objects.requireNonNull(rowType, "rowType");
        validateRowType(rowType);
        this.arrayType = (ArrayType) rowType.getTypeAt(0);
        this.elementType = arrayType.getElementType();
        this.ignoreParseErrors = ignoreParseErrors;
    }

    private static void validateRowType(RowType rowType) {
        if (rowType.getFieldCount() != 1) {
            throw new ValidationException(
                    "Array JSON format requires the table schema to expose exactly one column " +
                    "of ARRAY type representing the MQTT payload.");
        }
        final LogicalType fieldType = rowType.getTypeAt(0);
        if (fieldType.getTypeRoot() != LogicalTypeRoot.ARRAY) {
            throw new ValidationException(
                    "Array JSON format requires the single column to be declared as ARRAY, current type=" + fieldType);
        }
    }

    @Override
    public void open(InitializationContext context) {
        // nothing to initialize
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        if (message == null) {
            return null;
        }
        try {
            final JsonNode root = OBJECT_MAPPER.readTree(message);
            if (root == null || root.isNull()) {
                return null;
            }
            if (!root.isArray()) {
                throw new ValidationException("Expected JSON array as root, but found: " + root.getNodeType());
            }
            final ArrayData converted = convertArray((ArrayNode) root, elementType);
            return GenericRowData.of(converted);
        } catch (Exception ex) {
            if (ignoreParseErrors) {
                return null;
            }
            if (ex instanceof IOException) {
                throw (IOException) ex;
            }
            throw new IOException("Failed to deserialize JSON array payload.", ex);
        }
    }

    private ArrayData convertArray(ArrayNode node, LogicalType elementType) {
        final List<Object> elements = new ArrayList<>(node.size());
        for (JsonNode child : node) {
            elements.add(convertValue(child, elementType));
        }
        return new GenericArrayData(elements.toArray());
    }

    private Object convertValue(JsonNode node, LogicalType logicalType) {
        if (node == null || node.isNull()) {
            return null;
        }
        final LogicalTypeRoot root = logicalType.getTypeRoot();
        switch (root) {
            case CHAR:
            case VARCHAR:
                return StringData.fromString(node.asText());
            case BOOLEAN:
                return node.asBoolean();
            case TINYINT:
                return (byte) intValue(node, root);
            case SMALLINT:
                return (short) intValue(node, root);
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return intValue(node, root);
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return longValue(node);
            case FLOAT:
                return floatValue(node);
            case DOUBLE:
                return doubleValue(node);
            case DECIMAL:
                final DecimalType decimalType = (DecimalType) logicalType;
                return DecimalData.fromBigDecimal(
                        new BigDecimal(node.asText()),
                        decimalType.getPrecision(),
                        decimalType.getScale());
            case ARRAY:
                if (!node.isArray()) {
                    throw new ValidationException("Expected JSON array for ARRAY type but found: " + node.getNodeType());
                }
                return convertArray((ArrayNode) node, ((ArrayType) logicalType).getElementType());
            case ROW:
                if (!node.isObject()) {
                    throw new ValidationException("Expected JSON object for ROW type but found: " + node.getNodeType());
                }
                return convertRow(node, (RowType) logicalType);
            case MAP:
                if (!node.isObject()) {
                    throw new ValidationException("Expected JSON object for MAP type but found: " + node.getNodeType());
                }
                return convertMap(node, (MapType) logicalType);
            case VARBINARY:
            case BINARY:
                return convertBinary(node, logicalType);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return convertTimestamp(node);
            default:
                throw new ValidationException("Unsupported logical type for array-json format: " + logicalType);
        }
    }

    private static int intValue(JsonNode node, LogicalTypeRoot root) {
        if (node.isInt() || node.isShort() || node.isLong()) {
            return node.intValue();
        }
        final String text = node.asText();
        try {
            return Integer.parseInt(text);
        } catch (NumberFormatException ex) {
            throw new ValidationException("Failed to parse " + root + " from value: " + text, ex);
        }
    }

    private static long longValue(JsonNode node) {
        if (node.isLong() || node.isInt()) {
            return node.longValue();
        }
        final String text = node.asText();
        try {
            return Long.parseLong(text);
        } catch (NumberFormatException ex) {
            throw new ValidationException("Failed to parse BIGINT from value: " + text, ex);
        }
    }

    private static float floatValue(JsonNode node) {
        if (node.isFloat() || node.isDouble()) {
            return node.floatValue();
        }
        final String text = node.asText();
        try {
            return Float.parseFloat(text);
        } catch (NumberFormatException ex) {
            throw new ValidationException("Failed to parse FLOAT from value: " + text, ex);
        }
    }

    private static double doubleValue(JsonNode node) {
        if (node.isDouble() || node.isFloat()) {
            return node.doubleValue();
        }
        final String text = node.asText();
        try {
            return Double.parseDouble(text);
        } catch (NumberFormatException ex) {
            throw new ValidationException("Failed to parse DOUBLE from value: " + text, ex);
        }
    }

    private RowData convertRow(JsonNode node, RowType type) {
        final List<RowType.RowField> fields = type.getFields();
        final GenericRowData rowData = new GenericRowData(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            final RowType.RowField field = fields.get(i);
            final JsonNode child = node.get(field.getName());
            rowData.setField(i, convertValue(child, field.getType()));
        }
        return rowData;
    }

    private MapData convertMap(JsonNode node, MapType mapType) {
        final LogicalTypeRoot keyRoot = mapType.getKeyType().getTypeRoot();
        if (keyRoot != LogicalTypeRoot.VARCHAR && keyRoot != LogicalTypeRoot.CHAR) {
            throw new ValidationException("Only STRING map keys are supported for array-json format.");
        }
        final Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
        final LinkedHashMap<StringData, Object> map = new LinkedHashMap<>();
        while (fields.hasNext()) {
            final Map.Entry<String, JsonNode> entry = fields.next();
            map.put(
                    StringData.fromString(entry.getKey()),
                    convertValue(entry.getValue(), mapType.getValueType()));
        }
        return new GenericMapData(map);
    }

    private static Object convertBinary(JsonNode node, LogicalType logicalType) {
        if (node.isBinary()) {
            try {
                return node.binaryValue();
            } catch (IOException e) {
                throw new ValidationException("Failed to read binary value.", e);
            }
        }
        final byte[] bytes = node.asText().getBytes(StandardCharsets.UTF_8);
        if (logicalType instanceof VarBinaryType) {
            final int len = ((VarBinaryType) logicalType).getLength();
            if (len > 0 && bytes.length > len) {
                throw new ValidationException(
                        "Binary value exceeds declared VARBINARY(" + len + ") length: " + bytes.length);
            }
        } else if (logicalType instanceof BinaryType) {
            final int len = ((BinaryType) logicalType).getLength();
            if (len > 0 && bytes.length != len) {
                throw new ValidationException(
                        "Binary value must match declared BINARY(" + len + ") length: " + bytes.length);
            }
        }
        return bytes;
    }

    private static TimestampData convertTimestamp(JsonNode node) {
        final String text = node.asText();
        if (StringUtils.isNullOrWhitespaceOnly(text)) {
            return null;
        }
        try {
            final Instant instant = Instant.parse(text);
            return TimestampData.fromInstant(instant);
        } catch (DateTimeParseException ex) {
            try {
                final LocalDateTime localDateTime = LocalDateTime.parse(text);
                return TimestampData.fromLocalDateTime(localDateTime);
            } catch (DateTimeParseException inner) {
                throw new ValidationException("Unsupported timestamp literal: " + text, inner);
            }
        }
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return InternalTypeInfo.of(rowType);
    }
}
