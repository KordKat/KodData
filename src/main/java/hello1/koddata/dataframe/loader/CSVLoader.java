package hello1.koddata.dataframe.loader;

import hello1.koddata.dataframe.Column;
import hello1.koddata.dataframe.VariableElement;
import hello1.koddata.exception.KException;
import hello1.koddata.io.BufferedInputStreamPromax;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CSVLoader extends DataFrameLoader {

    private final String memoryGroupName;
    private List<Column> columns = List.of();

    public CSVLoader(String memoryGroupName) {
        this.memoryGroupName = memoryGroupName;
    }

    public List<Column> getColumns() {
        return columns;
    }

    @Override
    public void load(InputStream in) throws IOException {
        try (BufferedInputStreamPromax bis = new BufferedInputStreamPromax(in, 8192)) {

            List<String> lines = readAllLines(bis);
            if (lines.isEmpty()) {
                columns = List.of();
                return;
            }

            String[] columnNames = splitCsvLine(lines.get(0));
            int columnCount = columnNames.length;

            int rowCount = lines.size() - 1;
            String[][] cells = new String[rowCount][columnCount];

            for (int r = 0; r < rowCount; r++) {
                String[] parts = splitCsvLine(lines.get(r + 1));
                for (int c = 0; c < columnCount; c++) {
                    String value = c < parts.length ? trimQuotes(parts[c]) : null;
                    if (value != null && value.trim().isEmpty()) value = null;
                    cells[r][c] = value;
                }
            }

            ColumnKind[] kinds = inferColumnKinds(cells, columnCount, rowCount);

            List<Column> result = new ArrayList<>(columnCount);

            for (int c = 0; c < columnCount; c++) {
                String name = columnNames[c];
                ColumnKind kind = kinds[c];

                try {
                    Column column;
                    switch (kind) {
                        case SCALAR_INT ->
                                column = buildScalarIntColumn(name, cells, c, rowCount);
                        case SCALAR_DOUBLE ->
                                column = buildScalarDoubleColumn(name, cells, c, rowCount);
                        case SCALAR_STRING ->
                                column = buildScalarStringColumn(name, cells, c, rowCount);
                        case LIST_INT ->
                                column = buildListFixedNumericColumn(name, cells, c, rowCount, true);
                        case LIST_DOUBLE ->
                                column = buildListFixedNumericColumn(name, cells, c, rowCount, false);
                        case LIST_STRING ->
                                column = buildListStringColumn(name, cells, c, rowCount);
                        default -> throw new IllegalStateException();
                    }
                    result.add(column);
                } catch (KException e) {
                    throw new RuntimeException("Failed column: " + name, e);
                }
            }

            this.columns = result;
        }
    }

    private enum ColumnKind {
        SCALAR_INT,
        SCALAR_DOUBLE,
        SCALAR_STRING,
        LIST_INT,
        LIST_DOUBLE,
        LIST_STRING
    }

    private Column buildScalarIntColumn(String name, String[][] cells, int colIdx, int rowCount) throws KException {
        int elementSize = Integer.BYTES;
        ByteBuffer buffer = ByteBuffer.allocate(rowCount * elementSize);
        boolean[] flags = new boolean[rowCount];

        for (int r = 0; r < rowCount; r++) {
            String v = cells[r][colIdx];
            if (v != null) {
                flags[r] = true;
                buffer.putInt(Integer.parseInt(v));
            } else {
                buffer.putInt(0);
            }
        }
        buffer.flip();

        return new Column(name, elementSize, memoryGroupName, buffer, flags, elementSize, 0, rowCount);
    }

    private Column buildScalarDoubleColumn(String name, String[][] cells, int colIdx, int rowCount) throws KException {
        int elementSize = Double.BYTES;
        ByteBuffer buffer = ByteBuffer.allocate(rowCount * elementSize);
        boolean[] flags = new boolean[rowCount];

        for (int r = 0; r < rowCount; r++) {
            String v = cells[r][colIdx];
            if (v != null) {
                flags[r] = true;
                buffer.putDouble(Double.parseDouble(v));
            } else {
                buffer.putDouble(0.0);
            }
        }
        buffer.flip();

        return new Column(name, elementSize, memoryGroupName, buffer, flags, elementSize, 0, rowCount);
    }

    private Column buildScalarStringColumn(String name, String[][] cells, int colIdx, int rowCount) throws KException {
        List<VariableElement> list = new ArrayList<>(rowCount);
        boolean[] flags = new boolean[rowCount];

        for (int r = 0; r < rowCount; r++) {
            String v = cells[r][colIdx];
            if (v != null) {
                flags[r] = true;
                list.add(VariableElement.newStringElement(v));
            } else {
                list.add(VariableElement.newElement(new byte[0]));
            }
        }

        return new Column(name, list, memoryGroupName, flags, 0, rowCount);
    }

    private Column buildListFixedNumericColumn(String name, String[][] cells, int colIdx, int rowCount, boolean isInt)
            throws KException {

        int elementSize = isInt ? Integer.BYTES : Double.BYTES;
        List<List<byte[]>> lists = new ArrayList<>(rowCount);
        List<boolean[]> perFlags = new ArrayList<>(rowCount);
        boolean[] colFlags = new boolean[rowCount];

        for (int r = 0; r < rowCount; r++) {
            String cell = cells[r][colIdx];
            if (cell == null || cell.trim().isEmpty()) {
                colFlags[r] = false;
                lists.add(List.of());
                perFlags.add(new boolean[0]);
                continue;
            }

            colFlags[r] = true;

            String[] parts = cell.split("\\|");
            List<byte[]> rowList = new ArrayList<>(parts.length);
            boolean[] flags = new boolean[parts.length];

            for (int j = 0; j < parts.length; j++) {
                String item = parts[j].trim();
                if (item.isEmpty()) {
                    flags[j] = false;
                    rowList.add(new byte[elementSize]);
                    continue;
                }

                flags[j] = true;
                ByteBuffer bb = ByteBuffer.allocate(elementSize);
                if (isInt) bb.putInt(Integer.parseInt(item));
                else bb.putDouble(Double.parseDouble(item));
                rowList.add(bb.array());
            }

            lists.add(rowList);
            perFlags.add(flags);
        }

        return new Column(name, memoryGroupName, lists, perFlags, colFlags, elementSize, 0, rowCount);
    }

    private Column buildListStringColumn(String name, String[][] cells, int colIdx, int rowCount)
            throws KException {

        List<List<VariableElement>> lists = new ArrayList<>(rowCount);
        List<boolean[]> perFlags = new ArrayList<>(rowCount);
        boolean[] colFlags = new boolean[rowCount];

        for (int r = 0; r < rowCount; r++) {
            String cell = cells[r][colIdx];
            if (cell == null || cell.trim().isEmpty()) {
                colFlags[r] = false;
                lists.add(List.of());
                perFlags.add(new boolean[0]);
                continue;
            }

            colFlags[r] = true;

            String[] parts = cell.split("\\|");
            List<VariableElement> rowList = new ArrayList<>(parts.length);
            boolean[] flags = new boolean[parts.length];

            for (int j = 0; j < parts.length; j++) {
                String item = parts[j].trim();
                if (item.isEmpty()) {
                    flags[j] = false;
                    rowList.add(VariableElement.newElement(new byte[0]));
                    continue;
                }
                flags[j] = true;
                rowList.add(VariableElement.newStringElement(item));
            }

            lists.add(rowList);
            perFlags.add(flags);
        }

        return new Column(name, memoryGroupName, lists, perFlags, colFlags, 0, rowCount);
    }

    private ColumnKind[] inferColumnKinds(String[][] cells, int columnCount, int rowCount) {
        ColumnKind[] kinds = new ColumnKind[columnCount];

        for (int c = 0; c < columnCount; c++) {
            boolean isList = false;

            for (int r = 0; r < rowCount; r++) {
                String v = cells[r][c];
                if (v != null && v.contains("|")) {
                    isList = true;
                    break;
                }
            }

            if (!isList) kinds[c] = inferScalarKind(cells, c, rowCount);
            else kinds[c] = inferListKind(cells, c, rowCount);
        }

        return kinds;
    }

    private ColumnKind inferScalarKind(String[][] cells, int colIdx, int rowCount) {
        boolean canInt = true;
        boolean canDouble = true;

        for (int r = 0; r < rowCount; r++) {
            String v = cells[r][colIdx];
            if (v == null) continue;

            if (canInt) {
                try { Integer.parseInt(v); }
                catch (Exception e) { canInt = false; }
            }

            if (canDouble) {
                try { Double.parseDouble(v); }
                catch (Exception e) { canDouble = false; }
            }

            if (!canInt && !canDouble) break;
        }

        if (canInt) return ColumnKind.SCALAR_INT;
        if (canDouble) return ColumnKind.SCALAR_DOUBLE;
        return ColumnKind.SCALAR_STRING;
    }

    private ColumnKind inferListKind(String[][] cells, int colIdx, int rowCount) {
        boolean canInt = true;
        boolean canDouble = true;

        for (int r = 0; r < rowCount; r++) {
            String cell = cells[r][colIdx];
            if (cell == null || cell.trim().isEmpty()) continue;

            String[] parts = cell.split("\\|");

            for (String p : parts) {
                String v = p.trim();
                if (v.isEmpty()) continue;

                if (canInt) {
                    try { Integer.parseInt(v); }
                    catch (Exception e) { canInt = false; }
                }

                if (canDouble) {
                    try { Double.parseDouble(v); }
                    catch (Exception e) { canDouble = false; }
                }
            }

            if (!canInt && !canDouble) break;
        }

        if (canInt) return ColumnKind.LIST_INT;
        if (canDouble) return ColumnKind.LIST_DOUBLE;
        return ColumnKind.LIST_STRING;
    }

    private List<String> readAllLines(BufferedInputStreamPromax in) throws IOException {
        List<String> lines = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        int b;

        while ((b = in.read()) != -1) {
            if (b == '\n') {
                lines.add(sb.toString());
                sb.setLength(0);
            } else if (b != '\r') {
                sb.append((char) b);
            }
        }

        if (sb.length() > 0) lines.add(sb.toString());
        return lines;
    }

    private String[] splitCsvLine(String line) {
        if (line == null || line.isEmpty()) return new String[0];

        List<String> tokens = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char ch = line.charAt(i);

            if (ch == '"') inQuotes = !inQuotes;
            else if (ch == ',' && !inQuotes) {
                tokens.add(sb.toString());
                sb.setLength(0);
            } else sb.append(ch);
        }

        tokens.add(sb.toString());
        return tokens.toArray(new String[0]);
    }

    private String trimQuotes(String s) {
        if (s == null) return null;
        String t = s.trim();
        if (t.length() >= 2 && t.charAt(0) == '"' && t.charAt(t.length() - 1) == '"')
            return t.substring(1, t.length() - 1);
        return t;
    }
}
