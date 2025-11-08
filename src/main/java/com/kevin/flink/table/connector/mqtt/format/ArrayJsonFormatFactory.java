package com.kevin.flink.table.connector.mqtt.format;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.util.Collections;
import java.util.Set;

/**
 * Factory that exposes a JSON Array decoding format for MQTT payloads.
 *
 * <p>The format expects that the root of the MQTT payload is a JSON array whose elements
 * are mapped to the element type of the single ARRAY column declared in the table schema.
 */
public class ArrayJsonFormatFactory implements DeserializationFormatFactory {

    public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS =
            ConfigOptions.key("ignore-parse-errors")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Skip corrupt JSON payloads instead of failing the pipeline.");

    @Override
    public String factoryIdentifier() {
        return "array-json";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.singleton(IGNORE_PARSE_ERRORS);
    }

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
            DynamicTableFactory.Context context,
            ReadableConfig formatOptions) {

        FactoryUtil.validateFactoryOptions(this, formatOptions);

        final boolean ignoreParseErrors = formatOptions.get(IGNORE_PARSE_ERRORS);

        return new DecodingFormat<DeserializationSchema<RowData>>() {
            @Override
            public DeserializationSchema<RowData> createRuntimeDecoder(
                    DynamicTableSource.Context runtimeContext,
                    DataType producedDataType) {

                final RowType rowType = (RowType) producedDataType.getLogicalType();
                return new ArrayJsonRowDataDeserializationSchema(rowType, ignoreParseErrors);
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }
}
