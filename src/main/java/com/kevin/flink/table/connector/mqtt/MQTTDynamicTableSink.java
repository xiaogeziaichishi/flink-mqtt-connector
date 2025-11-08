package com.kevin.flink.table.connector.mqtt;

import com.kevin.flink.streaming.connectors.mqtt.MQTTMessage;
import com.kevin.flink.streaming.connectors.mqtt.MQTTStreamSink;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
//import org.apache.flink.table.data.RowKind;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.UserCodeClassLoader;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * MQTT dynamic table sink.
 */
public class MQTTDynamicTableSink implements DynamicTableSink {

    private final DataType physicalDataType;
    private final org.apache.flink.table.connector.format.EncodingFormat<SerializationSchema<RowData>> valueFormat;
    private final Map<String, String> mqttConfig;

    public MQTTDynamicTableSink(
            DataType physicalDataType,
            org.apache.flink.table.connector.format.EncodingFormat<SerializationSchema<RowData>> valueFormat,
            Map<String, String> mqttConfig) {
        this.physicalDataType = physicalDataType;
        this.valueFormat = valueFormat;
        this.mqttConfig = new HashMap<>(mqttConfig);
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        final SerializationSchema<RowData> serializer =
                valueFormat.createRuntimeEncoder(context, physicalDataType);
        final MQTTTableSinkFunction sinkFunction =
                new MQTTTableSinkFunction(new HashMap<>(mqttConfig), serializer);
        return SinkFunctionProvider.of(sinkFunction);
    }

    @Override
    public DynamicTableSink copy() {
        return new MQTTDynamicTableSink(physicalDataType, valueFormat, mqttConfig);
    }

    @Override
    public String asSummaryString() {
        return "MQTT-Table-Sink";
    }

    /**
     * Runtime sink function bridging {@link RowData} with {@link MQTTStreamSink}.
     */
    static class MQTTTableSinkFunction extends RichSinkFunction<RowData> {

        private final Map<String, String> config;
        private final SerializationSchema<RowData> serializer;
        private transient MQTTStreamSink delegate;
        private transient String topic;
        private transient int qos;
        private final AtomicInteger messageIdGenerator = new AtomicInteger();

        MQTTTableSinkFunction(Map<String, String> config, SerializationSchema<RowData> serializer) {
            this.config = config;
            this.serializer = serializer;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            topic = config.get("topic");
            if (topic == null) {
                throw new IllegalArgumentException("MQTT option `topic` must be provided.");
            }
            qos = Integer.parseInt(config.getOrDefault("QoS", "2"));
            serializer.open(createInitializationContext());
            delegate = new MQTTStreamSink(config);
            delegate.open(parameters);
        }

        @Override
        public void invoke(RowData value, Context context) throws Exception {
            if (value.getRowKind() != RowKind.INSERT) {
                return;
            }
            final byte[] payload;
            try {
                payload = serializer.serialize(value);
            } catch (Exception e) {
                throw new RuntimeException("Failed to serialize row for MQTT.", e);
            }
            final MQTTMessage message = new MQTTMessage();
            message.setTopic(topic);
            message.setPayload(payload);
            message.setQos(qos);
            message.setMessageId(nextMessageId());
            delegate.invoke(message, context);
        }

        @Override
        public void close() throws Exception {
            super.close();
            if (delegate != null) {
                delegate.close();
            }
        }

        private int nextMessageId() {
            int next = messageIdGenerator.updateAndGet(current -> {
                int candidate = current + 1;
                if (candidate > 0xFFFF) {
                    candidate = 1;
                }
                if (candidate <= 0) {
                    candidate = 1;
                }
                return candidate;
            });
            return next;
        }

        private SerializationSchema.InitializationContext createInitializationContext() {
            final MetricGroup metricGroup = getRuntimeContext().getMetricGroup();
            final UserCodeClassLoader classLoader = wrapUserCodeClassLoader(getRuntimeContext().getUserCodeClassLoader());
            return new SerializationSchema.InitializationContext() {
                @Override
                public MetricGroup getMetricGroup() {
                    return metricGroup;
                }

                @Override
                public UserCodeClassLoader getUserCodeClassLoader() {
                    return classLoader;
                }
            };
        }

        private static UserCodeClassLoader wrapUserCodeClassLoader(final ClassLoader classLoader) {
            if (classLoader instanceof UserCodeClassLoader) {
                return (UserCodeClassLoader) classLoader;
            }
            return new UserCodeClassLoader() {
                @Override
                public ClassLoader asClassLoader() {
                    return classLoader;
                }

                @Override
                public void registerReleaseHookIfAbsent(String name, Runnable hook) {
                    // no-op wrapper
                }
            };
        }
    }
}
