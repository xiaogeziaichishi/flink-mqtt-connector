package com.kevin.flink.table.connector.mqtt;

import com.kevin.flink.streaming.connectors.mqtt.MQTTMessage;
import com.kevin.flink.streaming.connectors.mqtt.MQTTStreamSource;
import com.kevin.flink.table.connector.mqtt.format.RawStringRowDataDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.UserCodeClassLoader;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * MQTT dynamic table source.
 */
public class MQTTDynamicTableSource implements ScanTableSource {

    private final DataType physicalDataType;
    private final RowType physicalRowType;
    private final Optional<org.apache.flink.table.connector.format.DecodingFormat<DeserializationSchema<RowData>>> valueFormat;
    private final Map<String, String> mqttConfig;

    public MQTTDynamicTableSource(
            DataType physicalDataType,
            Optional<org.apache.flink.table.connector.format.DecodingFormat<DeserializationSchema<RowData>>> valueFormat,
            Map<String, String> mqttConfig) {
        this.physicalDataType = physicalDataType;
        this.physicalRowType = (RowType) physicalDataType.getLogicalType();
        this.valueFormat = valueFormat;
        this.mqttConfig = new HashMap<>(mqttConfig);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        final DeserializationSchema<RowData> deserializer =
                valueFormat
                        .map(format -> format.createRuntimeDecoder(runtimeProviderContext, physicalDataType))
                        .orElseGet(this::createDefaultRawDeserializer);
        final MQTTTableSourceFunction sourceFunction =
                new MQTTTableSourceFunction(new HashMap<>(mqttConfig), deserializer, physicalRowType);
        return SourceFunctionProvider.of(sourceFunction, false);
    }

    private DeserializationSchema<RowData> createDefaultRawDeserializer() {
        return new RawStringRowDataDeserializationSchema(physicalRowType, (String) null);
    }

    @Override
    public DynamicTableSource copy() {
        return new MQTTDynamicTableSource(physicalDataType, valueFormat, mqttConfig);
    }

    @Override
    public String asSummaryString() {
        return "MQTT-Table-Source";
    }

    /**
     * Runtime source function bridging MQTTStreamSource with the {@link RowData} API.
     */
    static class MQTTTableSourceFunction extends RichParallelSourceFunction<RowData>
            implements org.apache.flink.streaming.api.checkpoint.CheckpointedFunction, ResultTypeQueryable<RowData> {

        private final Map<String, String> config;
        private final DeserializationSchema<RowData> deserializer;
        private final RowType producedRowType;
        private transient MQTTStreamSource delegate;

        MQTTTableSourceFunction(
                Map<String, String> config,
                DeserializationSchema<RowData> deserializer,
                RowType producedRowType) {
            this.config = config;
            this.deserializer = deserializer;
            this.producedRowType = producedRowType;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            if (delegate == null) {
                delegate = new MQTTStreamSource(config);
            }
            delegate.open(parameters);
            deserializer.open(createInitializationContext());
        }

        @Override
        public void run(SourceContext<RowData> ctx) throws Exception {
            delegate.run(new ForwardingSourceContext(ctx, deserializer));
        }

        @Override
        public void cancel() {
            if (delegate != null) {
                delegate.cancel();
            }
        }

        @Override
        public void close() throws Exception {
            super.close();
            if (delegate != null) {
                delegate.close();
            }
        }

        @Override
        public void snapshotState(org.apache.flink.runtime.state.FunctionSnapshotContext context) throws Exception {
            delegate.snapshotState(context);
        }

        @Override
        public void initializeState(org.apache.flink.runtime.state.FunctionInitializationContext context) throws Exception {
            if (delegate == null) {
                delegate = new MQTTStreamSource(config);
            }
            delegate.initializeState(context);
        }

        @Override
        public TypeInformation<RowData> getProducedType() {
            return org.apache.flink.table.runtime.typeutils.InternalTypeInfo.of(producedRowType);
        }

        private DeserializationSchema.InitializationContext createInitializationContext() {
            final MetricGroup metricGroup = getRuntimeContext().getMetricGroup();
            final UserCodeClassLoader classLoader = wrapUserCodeClassLoader(getRuntimeContext().getUserCodeClassLoader());
            return new DeserializationSchema.InitializationContext() {
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
                    // no-op for simple wrapper
                }
            };
        }
    }

    private static class ForwardingSourceContext implements SourceFunction.SourceContext<MQTTMessage> {

        private final SourceFunction.SourceContext<RowData> downstreamCtx;
        private final DeserializationSchema<RowData> deserializer;

        ForwardingSourceContext(
                SourceFunction.SourceContext<RowData> downstreamCtx,
                DeserializationSchema<RowData> deserializer) {
            this.downstreamCtx = downstreamCtx;
            this.deserializer = deserializer;
        }

        @Override
        public void collect(MQTTMessage message) {
            try {
                final RowData row = deserializer.deserialize(message.getPayload());
                if (row != null) {
                    downstreamCtx.collect(row);
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to deserialize MQTT payload.", e);
            }
        }

        @Override
        public void collectWithTimestamp(MQTTMessage element, long timestamp) {
            collect(element);
        }

        @Override
        public void emitWatermark(Watermark mark) {
            downstreamCtx.emitWatermark(mark);
        }

        @Override
        public void markAsTemporarilyIdle() {
            downstreamCtx.markAsTemporarilyIdle();
        }

        @Override
        public Object getCheckpointLock() {
            return downstreamCtx.getCheckpointLock();
        }

        @Override
        public void close() {
            downstreamCtx.close();
        }
    }
}
