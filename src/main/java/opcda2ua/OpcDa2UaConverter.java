package opcda2ua;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

/**
 * One-off tool that converts the resource Excel file into multiple OPC UA compatible sheets.
 */
public final class OpcDa2UaConverter {

    private static final Path DEFAULT_INPUT = Paths.get("src/main/resources/新增的4万多个实际28000新增.xlsx");
    private static final Path DEFAULT_OUTPUT_DIR = Paths.get("src/main/resources/out");
    private static final int MAX_ADDRESS = 1024;
    private static final int ARRAY_LENGTH = 1;
    private static final DataFormatter FORMATTER = new DataFormatter();
    private static final Map<String, Integer> TYPE_UNITS = new HashMap<>();
    private static final String[][] HEADER_TEMPLATE = {
            {"计算机名称/IP:", "win-nsfgtfsrm8o"},
            {"服务器名称:", "openPlant.OPC"}
    };
    private static final String[] TABLE_HEADER = {"序号", "变量名称", "变量类型", "数组长度", "单元地址"};

    static {
        TYPE_UNITS.put("BOOL", 1);
        TYPE_UNITS.put("R4", 4);
        TYPE_UNITS.put("I2", 2);
        TYPE_UNITS.put("I4", 4);
    }

    private OpcDa2UaConverter() {
    }

    public static void main(String[] args) throws Exception {
        Path input = args.length > 0 ? Paths.get(args[0]) : DEFAULT_INPUT;
        Path outputDir = args.length > 1 ? Paths.get(args[1]) : DEFAULT_OUTPUT_DIR;
        convert(input, outputDir);
    }

    private static void convert(Path input, Path outputDir) throws IOException {
        if (!Files.exists(input)) {
            throw new IllegalArgumentException("Input Excel not found: " + input.toAbsolutePath());
        }
        Files.createDirectories(outputDir);
        try (InputStream is = Files.newInputStream(input);
             Workbook workbook = WorkbookFactory.create(is)) {
            Sheet sheet = workbook.getSheetAt(0);
            List<RowData> rows = extractRows(sheet);
            if (rows.isEmpty()) {
                System.out.println("没有可转换的数据行");
                return;
            }
            writeOutputs(rows, outputDir);
        }
    }

    private static List<RowData> extractRows(Sheet sheet) {
        List<RowData> rows = new ArrayList<>();
        for (Row row : sheet) {
            if (row == null) {
                continue;
            }
            String variableName = readCell(row.getCell(0));
            String variableType = readCell(row.getCell(2));
            if (variableName.isEmpty() || variableType.isEmpty() || isHeaderValue(variableName)) {
                continue;
            }
            rows.add(new RowData(variableName, variableType));
        }
        return rows;
    }

    private static boolean isHeaderValue(String value) {
        String upper = value.toUpperCase(Locale.ROOT);
        return upper.contains("点名") || upper.contains("变量名称") || upper.contains("序号");
    }

    private static void writeOutputs(List<RowData> rows, Path outputDir) throws IOException {
        OutputFile current = new OutputFile(outputDir, 105);
        int nextAddress = 0;
        for (RowData row : rows) {
            int units = unitsFor(row.type());
            if (nextAddress + units > MAX_ADDRESS) {
                current.save();
                current = new OutputFile(outputDir, current.index + 1);
                nextAddress = 0;
            }
            current.writeRow(row, nextAddress);
            nextAddress += units;
        }
        current.save();
    }

    private static int unitsFor(String type) {
        return TYPE_UNITS.getOrDefault(type.toUpperCase(Locale.ROOT), 1);
    }

    private static String readCell(Cell cell) {
        if (cell == null) {
            return "";
        }
        return FORMATTER.formatCellValue(cell).trim();
    }

    private static final class RowData {
        private final String name;
        private final String type;

        private RowData(String name, String type) {
            this.name = name;
            this.type = type;
        }

        private String name() {
            return name;
        }

        private String type() {
            return type;
        }
    }

    private static final class OutputFile implements AutoCloseable {
        private final Workbook workbook;
        private final Sheet sheet;
        private final Path target;
        private final int index;
        private int nextRowIndex;
        private int sequence;

        private OutputFile(Path outputDir, int index) throws IOException {
            this.index = index;
            this.workbook = new XSSFWorkbook();
            this.sheet = workbook.createSheet("Sheet1");
            this.target = outputDir.resolve(String.format("变量文件%d.xlsx", index));
            this.nextRowIndex = seedTemplateRows();
            this.sequence = 1;
        }

        private int seedTemplateRows() {
            int rowIndex = 0;
            for (String[] templateRow : HEADER_TEMPLATE) {
                Row row = sheet.createRow(rowIndex++);
                row.createCell(0).setCellValue(templateRow[0]);
                row.createCell(1).setCellValue(templateRow.length > 1 ? templateRow[1] : "");
            }
            sheet.createRow(rowIndex++); // blank spacer row
            Row headerRow = sheet.createRow(rowIndex++);
            for (int i = 0; i < TABLE_HEADER.length; i++) {
                headerRow.createCell(i).setCellValue(TABLE_HEADER[i]);
            }
            return rowIndex;
        }

        private void writeRow(RowData data, int address) {
            Row row = sheet.createRow(nextRowIndex++);
            row.createCell(0).setCellValue(sequence++);
            row.createCell(1).setCellValue(data.name());
            row.createCell(2).setCellValue(data.type());
            row.createCell(3).setCellValue(ARRAY_LENGTH);
            row.createCell(4).setCellValue(address);
        }

        private void save() throws IOException {
            try (OutputStream os = Files.newOutputStream(target)) {
                workbook.write(os);
            }
            close();
            System.out.printf("写入 %s%n", target.toAbsolutePath());
        }

        @Override
        public void close() throws IOException {
            workbook.close();
        }
    }
}
