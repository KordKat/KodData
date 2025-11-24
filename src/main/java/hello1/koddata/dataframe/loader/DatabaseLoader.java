package hello1.koddata.dataframe.loader;

import hello1.koddata.dataframe.Column;
import hello1.koddata.dataframe.VariableElement;
import hello1.koddata.exception.KException;
import hello1.koddata.database.DatabaseConnection;
import hello1.koddata.utils.Either;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;

public class DatabaseLoader extends DataFrameLoader {

    private final DatabaseConnection conn;
    private final String query;
    private final String memoryGroupName;

    public DatabaseLoader(DatabaseConnection conn, String query, String memoryGroupName) {
        this.conn = conn;
        this.query = query;
        this.memoryGroupName = memoryGroupName;
    }

    @Override
    public void load(InputStream in) {
        try {
            conn.connect();
            Either<ResultSet, com.datastax.oss.driver.api.core.cql.ResultSet> res = conn.executeQuery(query);

            if (res.isLeft()) {
                loadJdbc(res.getLeft());
            } else {
                loadCql(res.getRight());
            }

            conn.close();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void loadJdbc(ResultSet rs) throws Exception {
        ResultSetMetaData meta = rs.getMetaData();
        int colCount = meta.getColumnCount();

        List<String> names = new ArrayList<>();
        for (int i = 1; i <= colCount; i++) {
            names.add(meta.getColumnName(i));
        }

        List<List<String>> table = new ArrayList<>();

        while (rs.next()) {
            List<String> row = new ArrayList<>(colCount);
            for (int i = 1; i <= colCount; i++) {
                Object v = rs.getObject(i);
                row.add(v == null ? null : v.toString());
            }
            table.add(row);
        }

        buildFromTable(names, table);
    }

    private void loadCql(com.datastax.oss.driver.api.core.cql.ResultSet cql) throws Exception {
        ColumnDefinitions definitions = cql.getColumnDefinitions();
        int colCount = definitions.size();

        List<String> names = new ArrayList<>();
        for (int i = 0; i < colCount; i++) {
            names.add(definitions.get(i).getName().asInternal());
        }

        List<List<String>> table = new ArrayList<>();
        for (Row r : cql) {
            List<String> row = new ArrayList<>(colCount);
            for (int i = 0; i < colCount; i++) {
                Object v = r.getObject(i);
                row.add(v == null ? null : v.toString());
            }
            table.add(row);
        }

        buildFromTable(names, table);
    }

    private void buildFromTable(List<String> names, List<List<String>> table) throws Exception {
        int rowCount = table.size();
        int colCount = names.size();

        String[][] cells = new String[rowCount][colCount];

        for (int r = 0; r < rowCount; r++) {
            List<String> row = table.get(r);
            for (int c = 0; c < colCount; c++) {
                cells[r][c] = row.get(c);
            }
        }

        ColumnKind[] kinds = inferColumnKinds(cells, colCount, rowCount);
        Column[] result = new Column[colCount];

        for (int c = 0; c < colCount; c++) {
            String name = names.get(c);
            ColumnKind kind = kinds[c];

            Column col = switch (kind) {
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

            result[c] = col;
        }

        this.columns = result;
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
        int sz = Integer.BYTES;
        ByteBuffer buf = ByteBuffer.allocate(rowCount * sz);
        boolean[] flags = new boolean[rowCount];

        for (int r = 0; r < rowCount; r++) {
            String v = cells[r][colIdx];
            if (v != null) {
                flags[r] = true;
                buf.putInt(Integer.parseInt(v));
            }
        }

        buf.flip();
        return new Column(name, sz, memoryGroupName, buf, flags, sz, 0, rowCount);
    }

    private Column buildScalarDoubleColumn(String name, String[][] cells, int colIdx, int rowCount) throws KException {
        int sz = Double.BYTES;
        ByteBuffer buf = ByteBuffer.allocate(rowCount * sz);
        boolean[] flags = new boolean[rowCount];

        for (int r = 0; r < rowCount; r++) {
            String v = cells[r][colIdx];
            if (v != null) {
                flags[r] = true;
                buf.putDouble(Double.parseDouble(v));
            }
        }

        buf.flip();
        return new Column(name, sz, memoryGroupName, buf, flags, sz, 0, rowCount);
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

    private Column buildScalarLogicalColumn(String name, String[][] cells, int colIdx, int rowCount) throws KException {
        int sz = 1;
        ByteBuffer buf = ByteBuffer.allocate(rowCount * sz);
        boolean[] flags = new boolean[rowCount];

        for (int r = 0; r < rowCount; r++) {
            String v = cells[r][colIdx];
            if (v != null) {
                flags[r] = true;
                boolean t = v.equalsIgnoreCase("true") || v.equals("1");
                buf.put((byte)(t ? 1 : 0));
            }
        }

        buf.flip();
        return new Column(name, sz, memoryGroupName, buf, flags, sz, 0, rowCount);
    }

    private Column buildScalarDateColumn(String name, String[][] cells, int colIdx, int rowCount) throws KException {
        int sz = Long.BYTES;
        ByteBuffer buf = ByteBuffer.allocate(rowCount * sz);
        boolean[] flags = new boolean[rowCount];

        for (int r = 0; r < rowCount; r++) {
            String v = cells[r][colIdx];
            if (v != null) {
                flags[r] = true;
                long epoch = java.time.LocalDate.parse(v).toEpochDay();
                buf.putLong(epoch);
            }
        }

        buf.flip();
        return new Column(name, sz, memoryGroupName, buf, flags, sz, 0, rowCount);
    }

    private Column buildScalarTimestampColumn(String name, String[][] cells, int colIdx, int rowCount) throws KException {
        int sz = Long.BYTES;
        ByteBuffer buf = ByteBuffer.allocate(rowCount * sz);
        boolean[] flags = new boolean[rowCount];

        for (int r = 0; r < rowCount; r++) {
            String v = cells[r][colIdx];
            if (v != null) {
                flags[r] = true;
                long epoch = java.time.Instant.parse(v).toEpochMilli();
                buf.putLong(epoch);
            }
        }

        buf.flip();
        return new Column(name, sz, memoryGroupName, buf, flags, sz, 0, rowCount);
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

        return new Column(name, memoryGroupName, lists, perFlags, colFlags, elementSize, 0, rowCount);
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

        return new Column(name, memoryGroupName, lists, perFlags, colFlags, elementSize, 0, rowCount);
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

        return new Column(name, memoryGroupName, lists, perFlags, colFlags, elementSize, 0, rowCount);
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

            try { java.time.LocalDate.parse(v.trim()); }
            catch (Exception e) { canDate = false; break; }
        }
        if (canDate) return ColumnKind.SCALAR_DATE;

        boolean canTimestamp = true;
        for (int r = 0; r < rowCount; r++) {
            String v = cells[r][colIdx];
            if (v == null) continue;

            try { java.time.Instant.parse(v.trim()); }
            catch (Exception e) { canTimestamp = false; break; }
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

        boolean canLogical = true;

        outer:
        for (int r = 0; r < rowCount; r++) {
            String cell = cells[r][colIdx];
            if (cell == null || cell.trim().isEmpty()) continue;

            for (String p : cell.split("\\|")) {
                String v = p.trim().toLowerCase();
                if (!(v.equals("true") || v.equals("false") || v.equals("1") || v.equals("0"))) {
                    canLogical = false;
                    break outer;
                }
            }
        }

        if (canLogical) return ColumnKind.LIST_LOGICAL;

        boolean canDate = true;

        outer2:
        for (int r = 0; r < rowCount; r++) {
            String cell = cells[r][colIdx];
            if (cell == null || cell.trim().isEmpty()) continue;

            for (String p : cell.split("\\|")) {
                try {
                    java.time.LocalDate.parse(p.trim());
                } catch (Exception e) {
                    canDate = false;
                    break outer2;
                }
            }
        }

        if (canDate) return ColumnKind.LIST_DATE;

        boolean canTimestamp = true;

        outer3:
        for (int r = 0; r < rowCount; r++) {
            String cell = cells[r][colIdx];
            if (cell == null || cell.trim().isEmpty()) continue;

            for (String p : cell.split("\\|")) {
                try {
                    java.time.Instant.parse(p.trim());
                } catch (Exception e) {
                    canTimestamp = false;
                    break outer3;
                }
            }
        }

        if (canTimestamp) return ColumnKind.LIST_TIMESTAMP;

        return ColumnKind.LIST_STRING;
    }
}
