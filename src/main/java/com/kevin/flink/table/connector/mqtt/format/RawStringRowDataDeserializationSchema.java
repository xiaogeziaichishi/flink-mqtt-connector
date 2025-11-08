package com.kevin.flink.table.connector.mqtt.format;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Objects;

/**
 * Deserializes raw MQTT payloads into a single STRING column {@link RowData}.
 */
public class RawStringRowDataDeserializationSchema implements DeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private final RowType rowType;
    private final String charsetName;
    private transient Charset charset;

    public RawStringRowDataDeserializationSchema(RowType rowType, String charsetName) {
        this.rowType = Objects.requireNonNull(rowType, "rowType");
        this.charsetName = normalizeCharsetName(charsetName);
        validateRowType(rowType);
    }

    private static void validateRowType(RowType rowType) {
        if (rowType.getFieldCount() != 1) {
            throw new ValidationException(
                    "Raw format requires exactly one column, current size=" + rowType.getFieldCount());
        }
        final LogicalTypeRoot root = rowType.getTypeAt(0).getTypeRoot();
        if (root != LogicalTypeRoot.VARCHAR && root != LogicalTypeRoot.CHAR) {
            throw new ValidationException(
                    "Raw format requires the single column to be STRING, current type=" + rowType.getTypeAt(0));
        }
    }

    private static String normalizeCharsetName(String candidate) {
        final String name = (candidate == null || candidate.isEmpty())
                ? StandardCharsets.UTF_8.name()
                : candidate;
        try {
            return Charset.forName(name).name();
        } catch (IllegalCharsetNameException | UnsupportedCharsetException ex) {
            throw new ValidationException("Unsupported charset: " + candidate, ex);
        }
    }

    private Charset ensureCharset() {
        if (charset == null) {
            charset = Charset.forName(charsetName);
        }
        return charset;
    }

    @Override
    public void open(InitializationContext context) {
        ensureCharset();
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        if (message == null) {
            return null;
        }
        final String decoded = new String(message, ensureCharset());
        return GenericRowData.of(StringData.fromString(decoded));
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
