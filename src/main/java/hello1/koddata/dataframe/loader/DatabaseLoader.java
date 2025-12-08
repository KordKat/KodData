package hello1.koddata.dataframe.loader;

import hello1.koddata.dataframe.Column;
import hello1.koddata.dataframe.ColumnMetaData;
import hello1.koddata.engine.Value;
import hello1.koddata.exception.KException;
import hello1.koddata.database.DatabaseConnection;
import hello1.koddata.utils.Either;

import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;

public class DatabaseLoader extends DataFrameLoader {

    private final DatabaseConnection conn;
    private final String query;

    public DatabaseLoader(DatabaseConnection conn, String query, String memoryGroupName) {
        this.conn = conn;
        this.query = query;
    }

    @Override
    public void load(InputStream in) {
        try {
            conn.connect();
            Either<ResultSet, com.datastax.oss.driver.api.core.cql.ResultSet> res =
                    conn.executeQuery(query);

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

        ColumnDefinitions defs = cql.getColumnDefinitions();
        int colCount = defs.size();

        List<String> names = new ArrayList<>();
        for (int i = 0; i < colCount; i++) {
            names.add(defs.get(i).getName().asInternal());
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
                case SCALAR_INT       -> buildScalarIntColumn(name, cells, c, rowCount);
                case SCALAR_DOUBLE    -> buildScalarDoubleColumn(name, cells, c, rowCount);
                case SCALAR_STRING    -> buildScalarStringColumn(name, cells, c, rowCount);
                case LIST_INT         -> buildListIntColumn(name, cells, c, rowCount);
                case LIST_DOUBLE      -> buildListDoubleColumn(name, cells, c, rowCount);
                case LIST_STRING      -> buildListStringColumn(name, cells, c, rowCount);
                case SCALAR_LOGICAL   -> buildScalarLogicalColumn(name, cells, c, rowCount);
                case SCALAR_DATE      -> buildScalarDateColumn(name, cells, c, rowCount);
                case SCALAR_TIMESTAMP -> buildScalarTimestampColumn(name, cells, c, rowCount);
                case LIST_LOGICAL     -> buildListLogicalColumn(name, cells, c, rowCount);
                case LIST_DATE        -> buildListDateColumn(name, cells, c, rowCount);
                case LIST_TIMESTAMP   -> buildListTimestampColumn(name, cells, c, rowCount);
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
}