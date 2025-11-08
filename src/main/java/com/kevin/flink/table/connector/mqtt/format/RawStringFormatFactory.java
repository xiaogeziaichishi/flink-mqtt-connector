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
 * Factory that exposes the custom raw STRING decoding format for MQTT payloads.
 */
public class RawStringFormatFactory implements DeserializationFormatFactory {

    public static final ConfigOption<String> CHARSET =
            ConfigOptions.key("charset")
                    .stringType()
                    .defaultValue("UTF-8")
                    .withDescription("Charset used for decoding the payload; defaults to UTF-8.");

    @Override
    public String factoryIdentifier() {
        return "mqtt-raw";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.singleton(CHARSET);
    }

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
            DynamicTableFactory.Context context,
            ReadableConfig formatOptions) {

        FactoryUtil.validateFactoryOptions(this, formatOptions);

        final String charsetName = formatOptions.get(CHARSET);

        return new DecodingFormat<DeserializationSchema<RowData>>() {
            @Override
            public DeserializationSchema<RowData> createRuntimeDecoder(
                    DynamicTableSource.Context runtimeContext,
                    DataType producedDataType) {

                final RowType rowType = (RowType) producedDataType.getLogicalType();
                return new RawStringRowDataDeserializationSchema(rowType, charsetName);
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }
}
