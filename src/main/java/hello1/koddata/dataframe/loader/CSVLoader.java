package hello1.koddata.dataframe.loader;

import hello1.koddata.dataframe.Column;
import hello1.koddata.dataframe.ColumnMetaData;
import hello1.koddata.dataframe.VariableElement;
import hello1.koddata.exception.KException;
import hello1.koddata.io.BufferedInputStreamPromax;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class CSVLoader extends DataFrameLoader {

    private final String memoryGroupName;

    public CSVLoader(String memoryGroupName) {
        this.memoryGroupName = memoryGroupName;
    }

    @Override
    public void load(InputStream in) throws IOException {
        try (BufferedInputStreamPromax bis = new BufferedInputStreamPromax(in, 8192)) {

            List<String> lines = readAllLines(bis);
            if (lines.isEmpty()) {
                return;
            }

            String[] columnNames = splitCsvLine(lines.getFirst());
            int columnCount = columnNames.length;

            int rowCount = lines.size();
            String[][] cells = new String[rowCount][columnCount];

            for (int r = 0; r < rowCount-1; r++) {
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
                        case SCALAR_INT -> buildScalarIntColumn(name, cells, c, rowCount);
                        case SCALAR_DOUBLE -> buildScalarDoubleColumn(name, cells, c, rowCount);
                        case SCALAR_STRING -> buildScalarStringColumn(name, cells, c, rowCount);
                        case LIST_INT -> buildListFixedNumericColumn(name, cells, c, rowCount, true);
                        case LIST_DOUBLE -> buildListFixedNumericColumn(name, cells, c, rowCount, false);
                        case LIST_STRING -> buildListStringColumn(name, cells, c, rowCount);


                        case SCALAR_LOGICAL -> buildScalarLogicalColumn(name, cells, c, rowCount);
                        case SCALAR_DATE -> buildScalarDateColumn(name, cells, c, rowCount);
                        case SCALAR_TIMESTAMP -> buildScalarTimestampColumn(name, cells, c, rowCount);
                        case LIST_LOGICAL -> buildListLogicalColumn(name, cells, c, rowCount);
                        case LIST_DATE -> buildListDateColumn(name, cells, c, rowCount);
                        case LIST_TIMESTAMP -> buildListTimestampColumn(name, cells, c, rowCount);

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
        LIST_TIMESTAMP;
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
            }

        }
        buffer.flip();

        return new Column(name, elementSize, memoryGroupName, buffer, flags, elementSize, 0, rowCount, ColumnMetaData.ColumnDType.SCALAR_INT);
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
            }
        }
        buffer.flip();

        return new Column(name, elementSize, memoryGroupName, buffer, flags, elementSize, 0, rowCount, ColumnMetaData.ColumnDType.SCALAR_DOUBLE);
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

        return new Column(name, list, memoryGroupName, flags, 0, rowCount, ColumnMetaData.ColumnDType.SCALAR_STRING);
    }
    private Column buildScalarLogicalColumn(String name, String[][] cells, int colIdx, int rowCount)
            throws KException {

        int elementSize = 1;
        ByteBuffer buffer = ByteBuffer.allocate(rowCount * elementSize);
        boolean[] flags = new boolean[rowCount];

        for (int r = 0; r < rowCount; r++) {
            String v = cells[r][colIdx];
            if (v != null) {
                flags[r] = true;
                String t = v.trim().toLowerCase();
                boolean val = t.equals("true") || t.equals("1");
                buffer.put((byte) (val ? 1 : 0));
            }
        }

        buffer.flip();
        return new Column(name, elementSize, memoryGroupName, buffer, flags, elementSize, 0, rowCount, ColumnMetaData.ColumnDType.SCALAR_LOGICAL);
    }

    private Column buildScalarDateColumn(String name, String[][] cells, int colIdx, int rowCount)
            throws KException {

        int elementSize = Long.BYTES;
        ByteBuffer buffer = ByteBuffer.allocate(rowCount * elementSize);
        boolean[] flags = new boolean[rowCount];

        for (int r = 0; r < rowCount; r++) {
            String v = cells[r][colIdx];
            if (v != null) {
                flags[r] = true;
                long epoch = java.time.LocalDate.parse(v.trim()).toEpochDay();
                buffer.putLong(epoch);
            }
        }

        buffer.flip();
        return new Column(name, elementSize, memoryGroupName, buffer, flags, elementSize, 0, rowCount, ColumnMetaData.ColumnDType.SCALAR_DATE);
    }

    private Column buildScalarTimestampColumn(String name, String[][] cells, int colIdx, int rowCount)
            throws KException {

        int elementSize = Long.BYTES;
        ByteBuffer buffer = ByteBuffer.allocate(rowCount * elementSize);
        boolean[] flags = new boolean[rowCount];

        for (int r = 0; r < rowCount; r++) {
            String v = cells[r][colIdx];
            if (v != null) {
                flags[r] = true;
                long epoch = java.time.Instant.parse(v.trim()).toEpochMilli();
                buffer.putLong(epoch);
            }
        }

        buffer.flip();
        return new Column(name, elementSize, memoryGroupName, buffer, flags, elementSize, 0, rowCount, ColumnMetaData.ColumnDType.SCALAR_TIMESTAMP);
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

        return new Column(name, memoryGroupName, lists, perFlags, colFlags, elementSize, 0, rowCount, ColumnMetaData.ColumnDType.LIST_DOUBLE);
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

        return new Column(name, memoryGroupName, lists, perFlags, colFlags, 0, rowCount, ColumnMetaData.ColumnDType.LIST_STRING);
    }


    private Column buildListLogicalColumn(String name, String[][] cells, int colIdx, int rowCount)
            throws KException {

        int elementSize = 1;
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
                String item = parts[j].trim().toLowerCase();

                if (item.isEmpty()) {
                    flags[j] = false;
                    rowList.add(new byte[elementSize]);
                    continue;
                }

                flags[j] = true;
                byte b = (byte)((item.equals("true") || item.equals("1")) ? 1 : 0);
                rowList.add(new byte[]{b});
            }
            lists.add(rowList);
            perFlags.add(flags);
        }
        return new Column(name, memoryGroupName, lists, perFlags, colFlags, elementSize, 0, rowCount, ColumnMetaData.ColumnDType.LIST_LOGICAL);
    }


    private Column buildListDateColumn(String name, String[][] cells, int colIdx, int rowCount)
            throws KException {

        int elementSize = Long.BYTES;
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
                long epoch = java.time.LocalDate.parse(item).toEpochDay();
                ByteBuffer bb = ByteBuffer.allocate(elementSize);
                bb.putLong(epoch);
                rowList.add(bb.array());
            }

            lists.add(rowList);
            perFlags.add(flags);
        }

        return new Column(name, memoryGroupName, lists, perFlags, colFlags, elementSize, 0, rowCount, ColumnMetaData.ColumnDType.LIST_DATE);
    }

    private Column buildListTimestampColumn(String name, String[][] cells, int colIdx, int rowCount)
            throws KException {

        int elementSize = Long.BYTES;
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
                long epoch = java.time.Instant.parse(item).toEpochMilli();
                ByteBuffer bb = ByteBuffer.allocate(elementSize);
                bb.putLong(epoch);
                rowList.add(bb.array());
            }

            lists.add(rowList);
            perFlags.add(flags);
        }

        return new Column(name, memoryGroupName, lists, perFlags, colFlags, elementSize, 0, rowCount, ColumnMetaData.ColumnDType.LIST_TIMESTAMP);
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

        if (canInt) return ColumnKind.LIST_INT;
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



    private List<String> readAllLines(BufferedInputStreamPromax in)
            throws IOException {
        List<String> lines = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        int b;
        int currentRow = 0;

        while ((b = in.read()) != -1) {

            if (b == '\n') {
                String line = sb.toString();
                sb.setLength(0);
                lines.add(line);
                currentRow++;
            }
            else if (b != '\r') {
                sb.append((char) b);
            }
        }
        if (!sb.isEmpty()) {
            lines.add(sb.toString());
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
