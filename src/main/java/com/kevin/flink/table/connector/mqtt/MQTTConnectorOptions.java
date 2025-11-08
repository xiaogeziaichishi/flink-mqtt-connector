package com.kevin.flink.table.connector.mqtt;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Connector specific options shared by the dynamic table source and sink.
 */
public final class MQTTConnectorOptions {

    public static final String IDENTIFIER = "mqtt";

    public static final ConfigOption<String> BROKER_URL =
            ConfigOptions.key("broker-url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("MQTT broker URL in the form tcp://host:port.");

    public static final ConfigOption<String> TOPIC =
            ConfigOptions.key("topic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("MQTT topic to subscribe to or publish into.");

    public static final ConfigOption<String> CLIENT_ID =
            ConfigOptions.key("client-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional client identifier used when connecting to the broker.");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Username used for broker authentication.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Password used for broker authentication.");

    public static final ConfigOption<String> PERSISTENCE =
            ConfigOptions.key("persistence")
                    .stringType()
                    .defaultValue("file")
                    .withDescription("Persistence mode `memory` or `file` (default).");

    public static final ConfigOption<String> LOCAL_STORAGE =
            ConfigOptions.key("local-storage")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Local directory used for file based MQTT persistence.");

    public static final ConfigOption<Integer> CONNECTION_TIMEOUT =
            ConfigOptions.key("connection-timeout")
                    .intType()
                    .defaultValue(30)
                    .withDescription("Connection timeout in seconds.");

    public static final ConfigOption<Integer> KEEP_ALIVE =
            ConfigOptions.key("keep-alive")
                    .intType()
                    .defaultValue(60)
                    .withDescription("Keep alive interval in seconds.");

    public static final ConfigOption<Integer> MQTT_VERSION =
            ConfigOptions.key("mqtt-version")
                    .intType()
                    .defaultValue(4)
                    .withDescription("MQTT protocol version to use.");

    public static final ConfigOption<Boolean> CLEAN_SESSION =
            ConfigOptions.key("clean-session")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to use clean sessions when connecting.");

    public static final ConfigOption<Integer> QOS =
            ConfigOptions.key("qos")
                    .intType()
                    .defaultValue(2)
                    .withDescription("Quality of Service level for publishing (0, 1, or 2).");

    public static final ConfigOption<Boolean> AUTO_RECONNECT =
            ConfigOptions.key("auto-reconnect")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether the client should automatically reconnect.");

    public static final ConfigOption<Integer> MAX_INFLIGHT =
            ConfigOptions.key("max-inflight")
                    .intType()
                    .defaultValue(60)
                    .withDescription("Maximum number of unacknowledged messages in flight.");

    public static final ConfigOption<Long> MAX_BATCH_MESSAGE_NUM =
            ConfigOptions.key("max-batch-message-num")
                    .longType()
                    .defaultValue(Long.MAX_VALUE)
                    .withDescription("Maximum number of cached messages per checkpoint.");

    public static final ConfigOption<Long> MAX_BATCH_MESSAGE_SIZE =
            ConfigOptions.key("max-batch-message-size")
                    .longType()
                    .defaultValue(Long.MAX_VALUE)
                    .withDescription("Maximum total size in bytes for cached messages.");

    public static final ConfigOption<Integer> MAX_RETRY_NUM =
            ConfigOptions.key("max-retry-num")
                    .intType()
                    .defaultValue(3)
                    .withDescription("Maximum number of retries when publishing fails.");

    // SSL options are collected generically but documented individually for clarity.
    static final String[] SSL_OPTION_KEYS = new String[]{
            "ssl.protocol",
            "ssl.key-store",
            "ssl.key-store-password",
            "ssl.key-store-type",
            "ssl.key-store-provider",
            "ssl.trust-store",
            "ssl.trust-store-password",
            "ssl.trust-store-type",
            "ssl.trust-store-provider",
            "ssl.ciphers",
            "ssl.key-manager",
            "ssl.trust-manager"
    };

    private MQTTConnectorOptions() {
        // utility class
    }

    static Map<String, String> buildMQTTProperties(ReadableConfig config) {
        final Map<String, String> mqttConfig = new HashMap<>();
        mqttConfig.put("brokerUrl", config.get(BROKER_URL));
        mqttConfig.put("topic", config.get(TOPIC));

        config.getOptional(CLIENT_ID).ifPresent(value -> mqttConfig.put("clientId", value));
        config.getOptional(USERNAME).ifPresent(value -> mqttConfig.put("username", value));
        config.getOptional(PASSWORD).ifPresent(value -> mqttConfig.put("password", value));

        mqttConfig.put("persistence", config.get(PERSISTENCE).toLowerCase(Locale.ROOT));
        config.getOptional(LOCAL_STORAGE).ifPresent(value -> mqttConfig.put("localStorage", value));
        mqttConfig.put("connectionTimeout", String.valueOf(config.get(CONNECTION_TIMEOUT)));
        mqttConfig.put("keepAlive", String.valueOf(config.get(KEEP_ALIVE)));
        mqttConfig.put("mqttVersion", String.valueOf(config.get(MQTT_VERSION)));
        mqttConfig.put("cleanSession", String.valueOf(config.get(CLEAN_SESSION)));
        mqttConfig.put("QoS", String.valueOf(config.get(QOS)));
        mqttConfig.put("autoReconnect", String.valueOf(config.get(AUTO_RECONNECT)));
        mqttConfig.put("maxInflight", String.valueOf(config.get(MAX_INFLIGHT)));
        mqttConfig.put("maxBatchMessageNum", String.valueOf(config.get(MAX_BATCH_MESSAGE_NUM)));
        mqttConfig.put("maxBatchMessageSize", String.valueOf(config.get(MAX_BATCH_MESSAGE_SIZE)));
        mqttConfig.put("maxRetryNum", String.valueOf(config.get(MAX_RETRY_NUM)));

        for (String key : SSL_OPTION_KEYS) {
            final ConfigOption<String> option = ConfigOptions.key(key).stringType().noDefaultValue();
            config.getOptional(option).ifPresent(value -> mqttConfig.put(key, value));
        }

        return mqttConfig;
    }
}
