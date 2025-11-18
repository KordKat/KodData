package hello1.koddata.database;

import com.datastax.oss.driver.api.core.CqlSession;

import java.sql.ResultSet;

public class CqlDatabaseConnection implements DatabaseConnection {

    private CqlSession session;

    public CqlDatabaseConnection(CqlSession session){

    }

    @Override
    public void connect() {

    }

    @Override
    public void close() {

    }

    @Override
    public ResultSet executeQuery(String query) {
        return null;
    }
}
