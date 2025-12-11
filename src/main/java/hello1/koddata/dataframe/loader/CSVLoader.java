package hello1.koddata.dataframe.loader;

import hello1.koddata.dataframe.Column;
import hello1.koddata.dataframe.ColumnMetaData;
import hello1.koddata.engine.Value;
import hello1.koddata.exception.KException;
import hello1.koddata.io.BufferedInputStreamPromax;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class CSVLoader extends DataFrameLoader {
    // Polymorphism

    @Override
    public void load(InputStream in) throws IOException {
        try (BufferedInputStreamPromax bis = new BufferedInputStreamPromax(in, 8192)) {

            List<String> lines = readAllLines(bis);
            if (lines.isEmpty()) {
                return;
            }
            String[] columnNames = splitCsvLine(lines.get(0));
            int columnCount = columnNames.length;
            int rowCount = lines.size() - 1;
            if (rowCount <= 0) {
                this.columns = new Column[0];
                return;
            }

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
            Column[] result = new Column[columnCount];

            for (int c = 0; c < columnCount; c++) {
                String name = columnNames[c];
                ColumnKind kind = kinds[c];

                try {
                    Column column = switch (kind) {
                        case SCALAR_INT      -> buildScalarIntColumn(name, cells, c, rowCount);
                        case SCALAR_DOUBLE   -> buildScalarDoubleColumn(name, cells, c, rowCount);
                        case SCALAR_STRING   -> buildScalarStringColumn(name, cells, c, rowCount);
                        case LIST_INT        -> buildListIntColumn(name, cells, c, rowCount);
                        case LIST_DOUBLE     -> buildListDoubleColumn(name, cells, c, rowCount);
                        case LIST_STRING     -> buildListStringColumn(name, cells, c, rowCount);
                        case SCALAR_LOGICAL  -> buildScalarLogicalColumn(name, cells, c, rowCount);
                        case SCALAR_DATE     -> buildScalarDateColumn(name, cells, c, rowCount);
                        case SCALAR_TIMESTAMP-> buildScalarTimestampColumn(name, cells, c, rowCount);
                        case LIST_LOGICAL    -> buildListLogicalColumn(name, cells, c, rowCount);
                        case LIST_DATE       -> buildListDateColumn(name, cells, c, rowCount);
                        case LIST_TIMESTAMP  -> buildListTimestampColumn(name, cells, c, rowCount);
                    };
                    result[c] = column;

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
        LIST_STRING,
        SCALAR_LOGICAL,
        SCALAR_DATE,
        SCALAR_TIMESTAMP,
        LIST_LOGICAL,
        LIST_DATE,
        LIST_TIMESTAMP
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

        if (canInt)    return ColumnKind.SCALAR_INT;
        if (canDouble) return ColumnKind.SCALAR_DOUBLE;

        boolean canLogical = true;
        for (int r = 0; r < rowCount; r++) {
            String v = cells[r][colIdx];
            if (v == null) continue;
            String t = v.trim().toLowerCase();
            if (!(t.equals("true") || t.equals("false") || t.equals("1") || t.equals("0"))) {
                canLogical = false;
                break;
            }
        }
        if (canLogical) return ColumnKind.SCALAR_LOGICAL;

        boolean canDate = true;
        for (int r = 0; r < rowCount; r++) {
            String v = cells[r][colIdx];
            if (v == null) continue;
            try {
                java.time.LocalDate.parse(v.trim());
            } catch (Exception e) {
                canDate = false;
                break;
            }
        }
        if (canDate) return ColumnKind.SCALAR_DATE;

        boolean canTimestamp = true;
        for (int r = 0; r < rowCount; r++) {
            String v = cells[r][colIdx];
            if (v == null) continue;
            try {
                java.time.Instant.parse(v.trim());
            } catch (Exception e) {
                canTimestamp = false;
                break;
            }
        }
        if (canTimestamp) return ColumnKind.SCALAR_TIMESTAMP;

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

        if (canInt)    return ColumnKind.LIST_INT;
        if (canDouble) return ColumnKind.LIST_DOUBLE;

        boolean canLogicalList = true;
        outerLogical:
        for (int r = 0; r < rowCount; r++) {
            String cell = cells[r][colIdx];
            if (cell == null || cell.trim().isEmpty()) continue;

            for (String p : cell.split("\\|")) {
                String v = p.trim().toLowerCase();
                if (!(v.equals("true") || v.equals("false") || v.equals("1") || v.equals("0"))) {
                    canLogicalList = false;
                    break outerLogical;
                }
            }
        }
        if (canLogicalList) return ColumnKind.LIST_LOGICAL;

        boolean canDateList = true;
        outerDate:
        for (int r = 0; r < rowCount; r++) {
            String cell = cells[r][colIdx];
            if (cell == null || cell.trim().isEmpty()) continue;

            for (String p : cell.split("\\|")) {
                try {
                    java.time.LocalDate.parse(p.trim());
                } catch (Exception e) {
                    canDateList = false;
                    break outerDate;
                }
            }
        }
        if (canDateList) return ColumnKind.LIST_DATE;

        boolean canTimestampList = true;
        outerTimestamp:
        for (int r = 0; r < rowCount; r++) {
            String cell = cells[r][colIdx];
            if (cell == null || cell.trim().isEmpty()) continue;

            for (String p : cell.split("\\|")) {
                try {
                    java.time.Instant.parse(p.trim());
                } catch (Exception e) {
                    canTimestampList = false;
                    break outerTimestamp;
                }
            }
        }
        if (canTimestampList) return ColumnKind.LIST_TIMESTAMP;

        return ColumnKind.LIST_STRING;
    }

    private Column buildScalarIntColumn(String name, String[][] cells, int colIdx, int rowCount) throws KException {
        List<Value<?>> data = new ArrayList<>(rowCount);
        for (int r = 0; r < rowCount; r++) {
            String v = cells[r][colIdx];
            data.add(new Value<>(v == null ? null : Integer.parseInt(v.trim())));
        }
        return new Column(name, data, ColumnMetaData.ColumnDType.SCALAR_INT);
    }

    private Column buildScalarDoubleColumn(String name, String[][] cells, int colIdx, int rowCount) throws KException {
        List<Value<?>> data = new ArrayList<>(rowCount);
        for (int r = 0; r < rowCount; r++) {
            String v = cells[r][colIdx];
            data.add(new Value<>(v == null ? null : Double.parseDouble(v.trim())));
        }
        return new Column(name, data, ColumnMetaData.ColumnDType.SCALAR_DOUBLE);
    }

    private Column buildScalarStringColumn(String name, String[][] cells, int colIdx, int rowCount) throws KException {
        List<Value<?>> data = new ArrayList<>(rowCount);
        for (int r = 0; r < rowCount; r++) {
            String v = cells[r][colIdx];
            data.add(new Value<>(v));
        }
        return new Column(name, data, ColumnMetaData.ColumnDType.SCALAR_STRING);
    }

    private Column buildScalarLogicalColumn(String name, String[][] cells, int colIdx, int rowCount) throws KException {
        List<Value<?>> data = new ArrayList<>(rowCount);
        for (int r = 0; r < rowCount; r++) {
            String v = cells[r][colIdx];
            if (v == null) {
                data.add(new Value<>(null));
            } else {
                String t = v.trim().toLowerCase();
                boolean val = t.equals("true") || t.equals("1");
                data.add(new Value<>(val));
            }
        }
        return new Column(name, data, ColumnMetaData.ColumnDType.SCALAR_LOGICAL);
    }

    private Column buildScalarDateColumn(String name, String[][] cells, int colIdx, int rowCount) throws KException {
        List<Value<?>> data = new ArrayList<>(rowCount);
        for (int r = 0; r < rowCount; r++) {
            String v = cells[r][colIdx];
            if (v == null) {
                data.add(new Value<>(null));
            } else {
                data.add(new Value<>(java.time.LocalDate.parse(v.trim())));
            }
        }
        return new Column(name, data, ColumnMetaData.ColumnDType.SCALAR_DATE);
    }

    private Column buildScalarTimestampColumn(String name, String[][] cells, int colIdx, int rowCount) throws KException {
        List<Value<?>> data = new ArrayList<>(rowCount);
        for (int r = 0; r < rowCount; r++) {
            String v = cells[r][colIdx];
            if (v == null) {
                data.add(new Value<>(null));
            } else {
                data.add(new Value<>(java.time.Instant.parse(v.trim())));
            }
        }
        return new Column(name, data, ColumnMetaData.ColumnDType.SCALAR_TIMESTAMP);
    }

    private Column buildListIntColumn(String name, String[][] cells, int colIdx, int rowCount) throws KException {
        List<Value<?>> data = new ArrayList<>(rowCount);

        for (int r = 0; r < rowCount; r++) {
            String cell = cells[r][colIdx];
            if (cell == null || cell.trim().isEmpty()) {
                data.add(new Value<>(null));
                continue;
            }

            String[] parts = cell.split("\\|");
            List<Integer> list = new ArrayList<>(parts.length);
            for (String p : parts) {
                String item = p.trim();
                if (item.isEmpty()) {
                    list.add(null);
                } else {
                    list.add(Integer.parseInt(item));
                }
            }
            data.add(new Value<>(list));
        }

        return new Column(name, data, ColumnMetaData.ColumnDType.LIST_INT);
    }

    private Column buildListDoubleColumn(String name, String[][] cells, int colIdx, int rowCount) throws KException {
        List<Value<?>> data = new ArrayList<>(rowCount);

        for (int r = 0; r < rowCount; r++) {
            String cell = cells[r][colIdx];
            if (cell == null || cell.trim().isEmpty()) {
                data.add(new Value<>(null));
                continue;
            }

            String[] parts = cell.split("\\|");
            List<Double> list = new ArrayList<>(parts.length);
            for (String p : parts) {
                String item = p.trim();
                if (item.isEmpty()) {
                    list.add(null);
                } else {
                    list.add(Double.parseDouble(item));
                }
            }
            data.add(new Value<>(list));
        }

        return new Column(name, data, ColumnMetaData.ColumnDType.LIST_DOUBLE);
    }

    private Column buildListStringColumn(String name, String[][] cells, int colIdx, int rowCount) throws KException {
        List<Value<?>> data = new ArrayList<>(rowCount);

        for (int r = 0; r < rowCount; r++) {
            String cell = cells[r][colIdx];
            if (cell == null || cell.trim().isEmpty()) {
                data.add(new Value<>(null));
                continue;
            }

            String[] parts = cell.split("\\|");
            List<String> list = new ArrayList<>(parts.length);
            for (String p : parts) {
                String item = p.trim();
                list.add(item.isEmpty() ? null : item);
            }
            data.add(new Value<>(list));
        }

        return new Column(name, data, ColumnMetaData.ColumnDType.LIST_STRING);
    }

    private Column buildListLogicalColumn(String name, String[][] cells, int colIdx, int rowCount) throws KException {
        List<Value<?>> data = new ArrayList<>(rowCount);

        for (int r = 0; r < rowCount; r++) {
            String cell = cells[r][colIdx];
            if (cell == null || cell.trim().isEmpty()) {
                data.add(new Value<>(null));
                continue;
            }

            String[] parts = cell.split("\\|");
            List<Boolean> list = new ArrayList<>(parts.length);
            for (String p : parts) {
                String item = p.trim().toLowerCase();
                if (item.isEmpty()) {
                    list.add(null);
                } else {
                    boolean val = item.equals("true") || item.equals("1");
                    list.add(val);
                }
            }
            data.add(new Value<>(list));
        }

        return new Column(name, data, ColumnMetaData.ColumnDType.LIST_LOGICAL);
    }

    private Column buildListDateColumn(String name, String[][] cells, int colIdx, int rowCount) throws KException {
        List<Value<?>> data = new ArrayList<>(rowCount);

        for (int r = 0; r < rowCount; r++) {
            String cell = cells[r][colIdx];
            if (cell == null || cell.trim().isEmpty()) {
                data.add(new Value<>(null));
                continue;
            }

            String[] parts = cell.split("\\|");
            List<java.time.LocalDate> list = new ArrayList<>(parts.length);
            for (String p : parts) {
                String item = p.trim();
                if (item.isEmpty()) {
                    list.add(null);
                } else {
                    list.add(java.time.LocalDate.parse(item));
                }
            }
            data.add(new Value<>(list));
        }

        return new Column(name, data, ColumnMetaData.ColumnDType.LIST_DATE);
    }

    private Column buildListTimestampColumn(String name, String[][] cells, int colIdx, int rowCount) throws KException {
        List<Value<?>> data = new ArrayList<>(rowCount);

        for (int r = 0; r < rowCount; r++) {
            String cell = cells[r][colIdx];
            if (cell == null || cell.trim().isEmpty()) {
                data.add(new Value<>(null));
                continue;
            }

            String[] parts = cell.split("\\|");
            List<java.time.Instant> list = new ArrayList<>(parts.length);
            for (String p : parts) {
                String item = p.trim();
                if (item.isEmpty()) {
                    list.add(null);
                } else {
                    list.add(java.time.Instant.parse(item));
                }
            }
            data.add(new Value<>(list));
        }

        return new Column(name, data, ColumnMetaData.ColumnDType.LIST_TIMESTAMP);
    }

    private List<String> readAllLines(BufferedInputStreamPromax in)
            throws IOException {
        List<String> lines = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        int b;

        while ((b = in.read()) != -1) {
            if (b == '\n') {
                String line = sb.toString();
                sb.setLength(0);

                if (!line.trim().isEmpty()) {
                    lines.add(line);
                }

            } else if (b != '\r') {
                sb.append((char) b);
            }
        }

        if (!sb.isEmpty()) {
            String last = sb.toString();
            if (!last.trim().isEmpty()) {
                lines.add(last);
            }
        }
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
