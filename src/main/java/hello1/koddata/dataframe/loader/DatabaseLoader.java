package hello1.koddata.dataframe.loader;

import hello1.koddata.database.DatabaseConnection;
import hello1.koddata.dataframe.*;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class DatabaseLoader extends DataFrameLoader {

    private final DatabaseConnection conn;

    public DatabaseLoader(DatabaseConnection conn) {
        this.conn = conn;
    }

    @Override
    public void load(InputStream in) {
        try {
            String sql = new String(in.readAllBytes(), StandardCharsets.UTF_8).trim();
            if (sql.isEmpty());

        } catch (Exception e) {
            frame = null;
            columns = new Column[0];
        }
    }
}
