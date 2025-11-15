## OPC DA Excel 分片转换工具说明

### 背景
- 输入文件：`src/main/resources/新增的4万多个实际28000新增.xlsx`，第一个 sheet。
- 只使用 **A 列（变量名称）** 和 **C 列（变量类型）** 数据；忽略表头和空值。
- 输出目录：`src/main/resources/out/`，每个工作簿都采用与示例图一致的表头（前两行计算机/服务器信息、一行空行、一行列标题）。

### 地址拆分逻辑
- 数组长度固定写 `1`。
- 类型占用单位：`BOOL=1`、`I2=2`、`I4=4`、`R4=4`，未知类型默认 1。
- 单元地址从 `0` 开始累加，直到即将超过 `1024` 时切换到下一个文件。
- 文件命名格式：`变量文件105.xlsx` 开始递增（105、106、107...），确保序号和示例保持一致。
- 序号列为每个文件内从 `1` 开始的递增值。

### 运行方法
```bash
mvn -q -DskipTests exec:java -Dexec.mainClass=opcda2ua.OpcDa2UaConverter
```

可选参数：
1. 自定义输入 Excel 路径。
2. 自定义输出目录。

示例：
```bash
mvn -q -DskipTests exec:java \
    -Dexec.mainClass=opcda2ua.OpcDa2UaConverter \
    -Dexec.args="src/main/resources/新增的4万多个实际28000新增.xlsx src/main/resources/out"
```

### 文件结构一览
- `pom.xml`：新增 `org.apache.poi:poi` 与 `poi-ooxml` 依赖，用于读写 Excel。
- `src/main/java/opcda2ua/OpcDa2UaConverter.java`：核心转换逻辑，负责读入、拆分和写出 Excel。
- `src/main/resources/out/`：输出目录（已加入 `.gitignore`，默认忽略）。

> 注意：该工具为一次性批处理脚本，可根据需要修改 `HEADER_TEMPLATE` 或 `TYPE_UNITS` 以适配不同项目。
