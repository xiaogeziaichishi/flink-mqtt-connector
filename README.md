# Flink Streaming MQTT Source 与 MQTT Sink

**简体中文** | [English](README.en.md)

这是一个针对 Flink Streaming 的 MQTT Source/Sink 连接器，可用于在实时作业中从 MQTT 服务器读取数据或写入数据。

该工程最初基于 Spark SQL Streaming MQTT Data Source https://github.com/kevin4936/kevin-flink-connector-mqtt3 ,改造而来，并继续面向 Flink 生态做增强。

- **新增了支持table的功能，方便flink-sql的使用**

- **实际用例可参考[此处](http://n150nas.bingxiangtie.top:8002/flink-mqtt/)(不过需要IPv6的IP才能访问)**
## 依赖方式

使用 SBT：

```scala
libraryDependencies += "io.github.kevin4936" %% "kevin-flink-connector-mqtt3" % "{{site.FLINK_VERSION}}"
```

或者：

```scala
libraryDependencies += "io.github.kevin4936" % "kevin-flink-connector-mqtt3_2.12" % "1.14.6"
```

使用 Maven：

```xml
<dependency>
    <groupId>io.github.kevin4936</groupId>
    <artifactId>kevin-flink-connector-mqtt3_2.12</artifactId>
    <version>1.14.6</version>
</dependency>
```

构建：

```bash
mvn clean install -DskipTests -Denforcer.skip=true
```

与 `--jars` 相比，使用 `--packages` 可以确保该依赖及其传递依赖被自动加入到 Flink classpath。

本库提供 Scala 2.12 版本，请根据自身 Scala 版本选择合适的坐标。


## Scala 示例

示例可参考(没有新增table功能，可参考本项目改造，不过Java/Scala没什么区别)：
https://github.com/kevin4936/flink-mqtt3

## 配置项

该连接器使用 [Eclipse Paho Java Client](https://eclipse.org/paho/clients/java/)，其 API 文档位于 [此处](http://www.eclipse.org/paho/files/javadoc/index.html)。

| 参数名                        | 说明                                                                                                                                                                                                                                                         | Eclipse Paho 对应 API                                                    |
|----------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------|
| `broker-url`               | MQTT 客户端连接的 URL，例如 `tcp://localhost:1883`、`ssl://localhost:1883`。                                                                                                                                                                                  |                                                                          |
| `persistence`              | 控制消息持久化方式。若值为 `memory`，重启后无法恢复。其他值表示持久化到磁盘，可搭配 `localStorage` 指定路径。                                                                                                                                                |                                                                          |
| `topic`                    | 订阅或写入的主题。                                                                                                                                                                                                                                           |                                                                          |
| `clientId`                 | 客户端唯一标识，用于 Source 端恢复。Sink 端由于任务可能并行执行，不会使用该 clientId。                                                                                                                                                                       |                                                                          |
| `qoS`                      | 订阅主题时的 QoS 上限。发布端 QoS 低于该值时按发布 QoS，发布端 QoS 高于该值时按订阅设置。                                                                                                                                                                   |                                                                          |
| `username`                 | 认证用户名（若服务端需要认证）。为空可能导致连接失败。                                                                                                                                                                                                       | `MqttConnectOptions.setUserName`                                         |
| `password`                 | 认证密码。                                                                                                                                                                                                                                                   | `MqttConnectOptions.setPassword`                                         |
| `cleanSession`             | 是否以全新 Session 启动。`true` 会清除上次运行保留的消息。默认 `false`。                                                                                                                                                                                     | `MqttConnectOptions.setCleanSession`                                     |
| `connectionTimeout`        | 连接超时时间，`0` 表示一直等待。                                                                                                                                                                                                                             | `MqttConnectOptions.setConnectionTimeout`                                |
| `keepAlive`                | Keep Alive 间隔（秒）。                                                                                                                                                                                                                                      | `MqttConnectOptions.setKeepAliveInterval`                                |
| `mqttVersion`              | 协议版本。                                                                                                                                                                                                                                                   | `MqttConnectOptions.setMqttVersion`                                      |
| `maxInflight`              | 最大 in-flight 请求数量，适合大吞吐场景。                                                                                                                                                                                                                    | `MqttConnectOptions.setMaxInflight`                                      |
| `autoReconnect`            | 是否在断线后自动重连。                                                                                                                                                                                                                                       | `MqttConnectOptions.setAutomaticReconnect`                               |
| `ssl.protocol`             | SSL 协议，例如 `SSLv3`、`TLS`、`TLSv1.2`。                                                                                                                                                                                                                    | `MqttConnectOptions.setSSLProperties`, `com.ibm.ssl.protocol`            |
| `ssl.key.store`            | KeyStore 文件绝对路径。                                                                                                                                                                                                                                      | `MqttConnectOptions.setSSLProperties`, `com.ibm.ssl.keyStore`            |
| `ssl.key.store.password`   | KeyStore 密码。                                                                                                                                                                                                                                              | `MqttConnectOptions.setSSLProperties`, `com.ibm.ssl.keyStorePassword`    |
| `ssl.key.store.type`       | KeyStore 类型，例如 `JKS`、`PKCS12`。                                                                                                                                                                                                                        | `MqttConnectOptions.setSSLProperties`, `com.ibm.ssl.keyStoreType`        |
| `ssl.key.store.provider`   | KeyStore Provider，例如 `IBMJCE`。                                                                                                                                                                                                                           | `MqttConnectOptions.setSSLProperties`, `com.ibm.ssl.keyStoreProvider`    |
| `ssl.trust.store`          | TrustStore 文件绝对路径。                                                                                                                                                                                                                                    | `MqttConnectOptions.setSSLProperties`, `com.ibm.ssl.trustStore`          |
| `ssl.trust.store.password` | TrustStore 密码。                                                                                                                                                                                                                                          | `MqttConnectOptions.setSSLProperties`, `com.ibm.ssl.trustStorePassword`  |
| `ssl.trust.store.type`     | TrustStore 类型。                                                                                                                                                                                                                                            | `MqttConnectOptions.setSSLProperties`, `com.ibm.ssl.trustStoreType`      |
| `ssl.trust.store.provider` | TrustStore Provider，例如 `IBMJCEFIPS`。                                                                                                                                                                                                                   | `MqttConnectOptions.setSSLProperties`, `com.ibm.ssl.trustStoreProvider`  |
| `ssl.ciphers`              | 启用的 Cipher 列表，例如 `SSL_RSA_WITH_AES_128_CBC_SHA`。                                                                                                                                                                                                     | `MqttConnectOptions.setSSLProperties`, `com.ibm.ssl.enabledCipherSuites` |

## Flink SQL 用法

`mqtt` 连接器以工厂的形式提供给 Table API / SQL。SQL 里的选项使用 kebab-case（如 `broker-url`），对应 Streaming 层的属性。

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

必选参数：

- `connector` = `mqtt`
- `broker-url`
- `topic`
- `format`

可选参数与流式配置一致：
`client-id`、`username`、`password`、`persistence`、`local-storage`、`connection-timeout`、`keep-alive`、`mqtt-version`、`clean-session`、`qos`、`auto-reconnect`、`max-inflight`、`max-batch-message-num`、`max-batch-message-size`、`max-retry-num` 以及所有 `ssl.*` 参数。

### raw 格式(自定义了mqtt-raw)
```sql
CREATE TABLE mqtt_in (
  payload STRING    -- 字段可声明成 STRING，即使原始 JSON 是 int 也能自动转换
) WITH (
  'connector'   = 'mqtt',                 -- 你的连接器 identifier
  'broker-url'  = 'tcp://192.168.0.193:1883', -- 本机 mosquitto；WSL/Docker 不通就换 host.docker.internal
  'topic'       = 'datahub/127/5021',
  'qos'         = '1',
  'client-id'   = 'flink-sql-123'  -- 防止 clientId 冲突，随便实现
  ,'format' = 'mqtt-raw'
);

CREATE TABLE print_out (
  payload STRING
) WITH ('connector'='print');

INSERT INTO print_out
SELECT payload FROM mqtt_in;
```
### json类型
**每一条要是json-object格式**

- mqtt输入格式

```json
{"a":"a1","b":2}
```
```sql
CREATE TABLE mqtt_in (
  a STRING,    -- 字段可声明成 STRING，即使原始 JSON 是 int 也能自动转换
  b int
) WITH (
  'connector'   = 'mqtt',                 -- 你的连接器 identifier
  'broker-url'  = 'tcp://192.168.0.193:1883', -- 本机 mosquitto；WSL/Docker 不通就换 host.docker.internal
  'topic'       = 'datahub/127/5021',
  'qos'         = '1',
  'client-id'   = 'flink-sql-123'  -- 防止 clientId 冲突，随便实现
  ,'format' = 'json'
);
SELECT a,b FROM mqtt_in;
```

**这种也支持**

- mqtt输入格式

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
  abc  STRING,                -- 字段可声明成 STRING，即使原始 JSON 是 int 也能自动转换
  xyz  STRING,                -- 如果想保留数值语义可以用 INT/DOUBLE，这里你说用 STRING 也可以
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
  'format'     = 'json',                  -- Flink 自带 json format factory
  'json.ignore-parse-errors' = 'true'     -- 可选：解析失败时丢弃
);
SELECT abc,xyz,items FROM mqtt_json_in;

-- items 字段被声明为 ARRAY<ROW<...>>，就可以自动把数组里的每个 JSON 对象解析成一行 ROW。
-- 如果你的 JSON 正好只有那三个字段（a/b/c），用 STRING 接收没问题；若需要 int，则把类型改成 INT，Flink 会直接用数值。
-- json 格式不需要你再单独写 factory，Flink Table API 自带；只要你在 SQL 里把 format 改成 json 并声明好 schema，就能正常解析。
```

输出：

```tex
          abc                            xyz                items
          top-value                     123                [(v1, v2, v3), (v4, v5, v6)]
```

### array-json 数组格式

`array-json` 是新增的解码格式，适用于 MQTT 消息的根节点是 JSON 数组的场景。例如：

```json
[
  {"a": "a1", "b": "b1"},
  {"a": "a2", "b": "b2"}
]
```

特性：

- 表结构必须只有一列，类型为 `ARRAY<...>`，数组元素类型可以是 `ROW`、`MAP`、`ARRAY`、以及常见的标量类型（STRING、数值、BOOLEAN、TIMESTAMP、BINARY、DECIMAL 等）。
- 可以通过 `array-json.ignore-parse-errors = 'true'` 控制遇到异常数据时是否跳过。
- 使用 SQL 的 `UNNEST` 将数组展开成多行，实现按业务字段灵活过滤、聚合。

#### 基础示例

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
FROM mqtt_source
         CROSS JOIN UNNEST(records) AS r;
-- 或者
SELECT a, b
FROM mqtt_source
         CROSS JOIN UNNEST(records) AS t(a, b);
```

如需解析不同字段名（如 `c`, `d`），只需调整 `ROW` 中的列定义，无需改动代码。

#### MAP + ARRAY 复合示例

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
CROSS JOIN UNNEST(records) AS r
WHERE r.metrics['temp'] > 36;
```

## 环境变量

可通过以下环境变量控制 Sink 端的 MQTT 行为：

- `flink.mqtt.client.connect.attempts`：尝试连接 MQTT Broker 的最大次数。
- `flink.mqtt.client.connect.backoff`：连接失败后的重试间隔（毫秒）。
- `flink.mqtt.connection.cache.timeout`：Sink 缓存的 MQTT 连接的空闲超时时间（毫秒），超时后会关闭。
- `flink.mqtt.client.publish.attempts`：消息发布失败时的最大重试次数。
- `flink.mqtt.client.publish.backoff`：发布失败后的重试间隔（毫秒）。

## 最佳实践

1. **借助 Kafka 提升可靠性**

> *MQTT 是为机器到机器（M2M）或物联网场景设计的极轻量级发布/订阅协议。*

MQTT 的设计重点是轻量，可靠性能力有限。对于需要较高可靠性的业务，可以使用 Kafka 作为缓冲：将多个 MQTT 数据源聚合到 Kafka，再由 Flink 从 Kafka 统一消费。Kafka 可扩展、可持久化，能显著提升整体可用性。

2. **多 MQTT 源异构场景**

若存在大量 schema、吞吐各不相同的 MQTT 源，直接为每个源构建独立流式作业会导致调度复杂、资源浪费。推荐的方法是：将所有来源的数据汇聚到单一 Kafka Topic，并在消息内写入用于区分来源的标签。Flink 端再依据标签选择对应的格式和解析逻辑，实现统一处理、统一运维。
