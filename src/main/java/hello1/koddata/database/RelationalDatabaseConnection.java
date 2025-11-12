package hello1.koddata.database;

import java.sql.ResultSet;

public class RelationalDatabaseConnection implements DatabaseConnection {
    private String host, user, pass;
    private int port;
    private String databaseName;
    private String dbmsName;

    public RelationalDatabaseConnection(String host, int port, String user, String pass, String dnName, String dbms){

    }

    @Override
    public void connect() {

    }

    @Override
    public void close() {

    }

    @Override
    public ResultSet executeQuery() {
        return null;
    }
}
