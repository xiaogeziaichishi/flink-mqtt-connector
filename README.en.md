# Flink Streaming MQTT Source and MQTT Sink

[简体中文](README.md) | **English**

This project provides MQTT source and sink connectors for Flink Streaming so that real-time jobs can read data from or write data to MQTT brokers. The implementation was adapted from the Spark SQL Streaming MQTT data source (https://github.com/kevin4936/kevin-flink-connector-mqtt3) and has been enhanced for the Flink ecosystem.

- **Table-format support has been added to make Flink SQL usage straightforward.**
- **A production-style showcase is available at [this link](http://n150nas.bingxiangtie.top:8002/flink-mqtt/) (requires IPv6 access).**

## Dependency Management

Using SBT:

```scala
libraryDependencies += "io.github.kevin4936" %% "kevin-flink-connector-mqtt3" % "{{site.FLINK_VERSION}}"
```

or

```scala
libraryDependencies += "io.github.kevin4936" % "kevin-flink-connector-mqtt3_2.12" % "1.14.6"
```

Using Maven:

```xml
<dependency>
    <groupId>io.github.kevin4936</groupId>
    <artifactId>kevin-flink-connector-mqtt3_2.12</artifactId>
    <version>1.14.6</version>
</dependency>
```

Build command:

```bash
mvn clean install -DskipTests -Denforcer.skip=true
```

Compared with `--jars`, using `--packages` ensures that this connector and all transitive dependencies are automatically added to the Flink classpath. The artifact is published for Scala 2.12, so choose coordinates that match your Scala version if necessary.

## Java Example

Full example: https://github.com/kevin4936/kevin-flink-connector-mqtt3/blob/main/src/test/java/com/kevin/flink/streaming/connectors/mqtt/MQTTStreamSourceAndSinkTest.java

## Scala Example

Full example: https://github.com/kevin4936/flink-mqtt3 (this repository does not yet contain the table-format enhancements; refer to this project for the extended functionality, but the Java/Scala APIs are otherwise identical).

## Configuration Options

The connector builds upon the [Eclipse Paho Java Client](https://eclipse.org/paho/clients/java/). Low-level API documentation can be found [here](http://www.eclipse.org/paho/files/javadoc/index.html).

| Parameter                | Description                                                                                                                                                                                                                                                   | Eclipse Paho Reference                                                   |
|--------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------|
| `brokerUrl`              | MQTT broker URL, e.g., `tcp://localhost:1883` or `ssl://localhost:1883`.                                                                                                                                                                                       |                                                                          |
| `persistence`            | Controls how incoming messages are persisted. `memory` disables recovery after restart; other values store data on disk and `localStorage` can point to a directory.                                                                                           |                                                                          |
| `topic`                  | Topic to subscribe (source) or publish (sink).                                                                                                                                                                                                                |                                                                          |
| `clientId`               | Unique client identifier used mainly by the source for recovery. The sink ignores it because tasks may run in parallel.                                                                                                                                      |                                                                          |
| `QoS`                    | Maximum QoS level per subscription. Messages published with lower QoS keep their own QoS; higher QoS values are downgraded.                                                                                                                                   |                                                                          |
| `username`               | Username for authentication (when required). Leaving it empty may trigger errors.                                                                                                                                                                             | `MqttConnectOptions.setUserName`                                         |
| `password`               | Password for authentication.                                                                                                                                                                                                                                  | `MqttConnectOptions.setPassword`                                         |
| `cleanSession`           | Starts a clean session when `true`, removing persisted messages from previous runs. Defaults to `false`.                                                                                                                                                      | `MqttConnectOptions.setCleanSession`                                     |
| `connectionTimeout`      | Connection timeout; `0` waits until the client connects.                                                                                                                                                                                                      | `MqttConnectOptions.setConnectionTimeout`                                |
| `keepAlive`              | Keep-alive interval in seconds.                                                                                                                                                                                                                                | `MqttConnectOptions.setKeepAliveInterval`                                |
| `mqttVersion`            | MQTT protocol version.                                                                                                                                                                                                                                        | `MqttConnectOptions.setMqttVersion`                                      |
| `maxInflight`            | Maximum in-flight requests, useful for high-throughput workloads.                                                                                                                                                                                             | `MqttConnectOptions.setMaxInflight`                                      |
| `autoReconnect`          | Automatically reconnect on connection loss.                                                                                                                                                                                                                  | `MqttConnectOptions.setAutomaticReconnect`                               |
| `ssl.protocol`           | SSL protocol such as `SSLv3`, `TLS`, `TLSv1.2`.                                                                                                                                                                                                               | `MqttConnectOptions.setSSLProperties`, `com.ibm.ssl.protocol`            |
| `ssl.key.store`          | Absolute path to the key store file.                                                                                                                                                                                                                          | `MqttConnectOptions.setSSLProperties`, `com.ibm.ssl.keyStore`            |
| `ssl.key.store.password` | Key store password.                                                                                                                                                                                                                                           | `MqttConnectOptions.setSSLProperties`, `com.ibm.ssl.keyStorePassword`    |
| `ssl.key.store.type`     | Key store type, e.g., `JKS`, `PKCS12`.                                                                                                                                                                                                                        | `MqttConnectOptions.setSSLProperties`, `com.ibm.ssl.keyStoreType`        |
| `ssl.key.store.provider` | Key store provider, e.g., `IBMJCE`.                                                                                                                                                                                                                           | `MqttConnectOptions.setSSLProperties`, `com.ibm.ssl.keyStoreProvider`    |
| `ssl.trust.store`        | Absolute path to the trust store file.                                                                                                                                                                                                                        | `MqttConnectOptions.setSSLProperties`, `com.ibm.ssl.trustStore`          |
| `ssl.trust.store.password` | Trust store password.                                                                                                                                                                                                                                       | `MqttConnectOptions.setSSLProperties`, `com.ibm.ssl.trustStorePassword`  |
| `ssl.trust.store.type`   | Trust store type.                                                                                                                                                                                                                                             | `MqttConnectOptions.setSSLProperties`, `com.ibm.ssl.trustStoreType`      |
| `ssl.trust.store.provider` | Trust store provider, e.g., `IBMJCEFIPS`.                                                                                                                                                                                                                   | `MqttConnectOptions.setSSLProperties`, `com.ibm.ssl.trustStoreProvider`  |
| `ssl.ciphers`            | Enabled cipher suites, e.g., `SSL_RSA_WITH_AES_128_CBC_SHA`.                                                                                                                                                                                                   | `MqttConnectOptions.setSSLProperties`, `com.ibm.ssl.enabledCipherSuites` |

## Flink SQL Usage

The `mqtt` connector is discoverable via the Table/SQL factory. SQL options use kebab-case (e.g., `broker-url`) and map to the streaming configuration.

```sql
CREATE TABLE mqtt_source (
    device STRING,
    reading DOUBLE,
    event_time TIMESTAMP(3)
) WITH (
    'connector' = 'mqtt',
    'broker-url' = 'tcp://broker:1883',
    'topic' = 'sensors/in',
    'format' = 'json'
);

CREATE TABLE mqtt_sink (
    device STRING,
    reading DOUBLE
) WITH (
    'connector' = 'mqtt',
    'broker-url' = 'tcp://broker:1883',
    'topic' = 'sensors/out',
    'format' = 'json'
);

INSERT INTO mqtt_sink
SELECT device, reading
FROM mqtt_source
WHERE reading > 42;
```

Required options:

- `connector` = `mqtt`
- `broker-url`
- `topic`
- `format`

Optional options match the streaming connector properties: `client-id`, `username`, `password`, `persistence`, `local-storage`, `connection-timeout`, `keep-alive`, `mqtt-version`, `clean-session`, `qos`, `auto-reconnect`, `max-inflight`, `max-batch-message-num`, `max-batch-message-size`, `max-retry-num`, plus all `ssl.*` keys.

### raw Format (custom `mqtt-raw`)

```sql
CREATE TABLE mqtt_in (
  payload STRING    -- declare STRING even if the original JSON field is numeric; Flink handles the cast
) WITH (
  'connector'   = 'mqtt',
  'broker-url'  = 'tcp://192.168.0.193:1883', -- replace with host.docker.internal if WSL/Docker cannot reach localhost
  'topic'       = 'datahub/127/5021',
  'qos'         = '1',
  'client-id'   = 'flink-sql-123',
  'format'      = 'mqtt-raw'
);

CREATE TABLE print_out (
  payload STRING
) WITH ('connector'='print');

INSERT INTO print_out
SELECT payload FROM mqtt_in;
```

### JSON Format

Each MQTT message must be a JSON object.

**Example payload**

```json
{"a":"a1","b":2}
```

```sql
CREATE TABLE mqtt_in (
  a STRING,
  b INT
) WITH (
  'connector'   = 'mqtt',
  'broker-url'  = 'tcp://192.168.0.193:1883',
  'topic'       = 'datahub/127/5021',
  'qos'         = '1',
  'client-id'   = 'flink-sql-123',
  'format'      = 'json'
);
SELECT a, b FROM mqtt_in;
```

**Nested structures are supported as well**

Payload:

```json
{
  "abc": "top-value",
  "xyz": 123,
  "items": [
    {"a": "v1", "b": "v2", "c": "v3"},
    {"a": "v4", "b": "v5", "c": "v6"}
  ]
}
```

```sql
CREATE TABLE mqtt_json_in (
  abc  STRING,
  xyz  STRING,
  items ARRAY<ROW<
    a STRING,
    b STRING,
    c STRING
  >>
) WITH (
  'connector'  = 'mqtt',
  'broker-url' = 'tcp://192.168.0.193:1883',
  'topic'      = 'datahub/127/5021',
  'qos'        = '1',
  'client-id'  = 'flink-sql-123-json',
  'format'     = 'json',
  'json.ignore-parse-errors' = 'true'
);
SELECT abc, xyz, items FROM mqtt_json_in;
```

Output:

```text
          abc                            xyz                items
          top-value                     123                [(v1, v2, v3), (v4, v5, v6)]
```

Notes:

- Because `items` is declared as `ARRAY<ROW<...>>`, each element in the JSON array maps to a row automatically.
- Fields can be typed as `STRING` even when the JSON literal is numeric; switch to `INT`/`DOUBLE` when you prefer numeric semantics.
- The `json` format factory ships with Flink; simply set `'format' = 'json'` and describe the schema in SQL—no custom factory is required.

### array-json Format

`array-json` is a custom decoding format for payloads whose root node is a JSON array, for example:

```json
[
  {"a": "a1", "b": "b1"},
  {"a": "a2", "b": "b2"}
]
```

Highlights:

- The table schema must expose exactly one column of type `ARRAY<...>`. The element type can be `ROW`, `MAP`, nested `ARRAY`, or supported scalar types (STRING, numeric, BOOLEAN, TIMESTAMP, BINARY, DECIMAL, etc.).
- `array-json.ignore-parse-errors = 'true'` skips corrupt payloads instead of failing the job.
- Use SQL `UNNEST` to explode each array element into its own row for filtering and aggregation.

#### Basic Example

```sql
CREATE TABLE mqtt_array_source (
    records ARRAY<ROW<a STRING, b STRING>>
) WITH (
    'connector' = 'mqtt',
    'broker-url' = 'tcp://127.0.0.1:1883',
    'topic' = 'demo/json-array',
    'format' = 'array-json',
    'array-json.ignore-parse-errors' = 'true'
);

SELECT r.a, r.b
FROM mqtt_array_source
CROSS JOIN UNNEST(records) AS t(r);
```

To handle different field names (e.g., `c`, `d`), adjust the `ROW` definition—no code changes are required.

#### MAP + ARRAY Example

```sql
CREATE TABLE mqtt_metrics (
    records ARRAY<ROW<
        deviceId STRING,
        metrics MAP<STRING, DOUBLE>
    >>
) WITH (
    'connector' = 'mqtt',
    'broker-url' = 'tcp://127.0.0.1:1883',
    'topic' = 'device/metrics',
    'format' = 'array-json'
);

SELECT
    r.deviceId,
    r.metrics['temp']     AS temp,
    r.metrics['pressure'] AS pressure
FROM mqtt_metrics
CROSS JOIN UNNEST(records) AS t(r)
WHERE r.metrics['temp'] > 36;
```

## Environment Variables

Control sink-side MQTT behavior with the following environment variables:

- `flink.mqtt.client.connect.attempts`: Maximum number of connection attempts before failing.
- `flink.mqtt.client.connect.backoff`: Delay (ms) between connection attempts.
- `flink.mqtt.connection.cache.timeout`: Idle timeout (ms) for cached MQTT connections.
- `flink.mqtt.client.publish.attempts`: Maximum number of publish retries.
- `flink.mqtt.client.publish.backoff`: Delay (ms) between publish retries.

## Best Practices

1. **Leverage Kafka for Reliability**

> *MQTT is an extremely lightweight publish/subscribe protocol designed for M2M/IoT scenarios.*

Because MQTT focuses on light weight, it lacks strong reliability guarantees. A common approach is to route MQTT data into Kafka and let Flink consume from Kafka, gaining durability and scalability.

2. **Handle Many Heterogeneous MQTT Sources**

Managing numerous MQTT sources with different schemas and throughput can make scheduling complex and waste resources. Instead, aggregate all streams into a single Kafka topic and embed a tag in each message to denote its origin. Flink can then dispatch messages by tag to the appropriate decoding and processing logic, simplifying operations.
