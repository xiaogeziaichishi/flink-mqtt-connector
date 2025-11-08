package com.kevin.flink.table.connector.mqtt;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Factory to create MQTT dynamic table source and sink.
 */
public class MQTTDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    @Override
    public String factoryIdentifier() {
        return MQTTConnectorOptions.IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(MQTTConnectorOptions.BROKER_URL);
        options.add(MQTTConnectorOptions.TOPIC);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FactoryUtil.FORMAT);
        options.add(MQTTConnectorOptions.CLIENT_ID);
        options.add(MQTTConnectorOptions.USERNAME);
        options.add(MQTTConnectorOptions.PASSWORD);
        options.add(MQTTConnectorOptions.PERSISTENCE);
        options.add(MQTTConnectorOptions.LOCAL_STORAGE);
        options.add(MQTTConnectorOptions.CONNECTION_TIMEOUT);
        options.add(MQTTConnectorOptions.KEEP_ALIVE);
        options.add(MQTTConnectorOptions.MQTT_VERSION);
        options.add(MQTTConnectorOptions.CLEAN_SESSION);
        options.add(MQTTConnectorOptions.QOS);
        options.add(MQTTConnectorOptions.AUTO_RECONNECT);
        options.add(MQTTConnectorOptions.MAX_INFLIGHT);
        options.add(MQTTConnectorOptions.MAX_BATCH_MESSAGE_NUM);
        options.add(MQTTConnectorOptions.MAX_BATCH_MESSAGE_SIZE);
        options.add(MQTTConnectorOptions.MAX_RETRY_NUM);
        for (String key : MQTTConnectorOptions.SSL_OPTION_KEYS) {
            options.add(ConfigOptions.key(key).stringType().noDefaultValue());
        }
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        final Optional<DecodingFormat<DeserializationSchema<RowData>>> valueFormat =
                helper.discoverOptionalDecodingFormat(DeserializationFormatFactory.class, FactoryUtil.FORMAT);
        helper.validate();

        final ReadableConfig options = helper.getOptions();
        final Map<String, String> mqttConfig = MQTTConnectorOptions.buildMQTTProperties(options);
        final DataType physicalDataType = context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();

        return new MQTTDynamicTableSource(physicalDataType, valueFormat, mqttConfig);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        final EncodingFormat<SerializationSchema<RowData>> valueFormat =
                helper.discoverEncodingFormat(SerializationFormatFactory.class, FactoryUtil.FORMAT);
        helper.validate();

        final ReadableConfig options = helper.getOptions();
        final Map<String, String> mqttConfig = MQTTConnectorOptions.buildMQTTProperties(options);
        final DataType physicalDataType = context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();

        return new MQTTDynamicTableSink(physicalDataType, valueFormat, mqttConfig);
    }
}
