package hello1.koddata.dataframe.loader;

import hello1.koddata.dataframe.Column;
import hello1.koddata.dataframe.DataFrame;
import hello1.koddata.dataframe.DataFrameSchema;
import hello1.koddata.dataframe.VariableElement;
import hello1.koddata.database.DatabaseConnection;

import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;

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

            if (query == null) {
                throw new RuntimeException("DatabaseLoader: query must not be null");
            }
            if (memoryGroupName == null) {
                throw new RuntimeException("DatabaseLoader: memoryGroupName must not be null");
            }

            conn.connect();
            ResultSet rs = conn.executeQuery(query);
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
                    Object obj = rs.getObject(i);
                    row.add(obj == null ? null : obj.toString());
                }
                table.add(row);
            }

            columns = new Column[colCount];
            String[] keys = new String[colCount];
            int[] sizes = new int[colCount];

            for (int c = 0; c < colCount; c++) {
                String name = names.get(c);
                boolean[] flags = new boolean[table.size()];
                List<VariableElement> values = new ArrayList<>(table.size());

                for (int r = 0; r < table.size(); r++) {
                    String v = table.get(r).get(c);
                    if (v != null) {
                        flags[r] = true;
                        values.add(VariableElement.newStringElement(v));
                    } else {
                        values.add(VariableElement.newElement(new byte[0]));
                    }
                }

                Column col = new Column(name, values, memoryGroupName, flags, 0, table.size());
                columns[c] = col;
                keys[c] = name;
                sizes[c] = -1;
            }

            DataFrameSchema schema = new DataFrameSchema(keys, sizes);
            frame = new DataFrame(schema, "database");

            conn.close();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
