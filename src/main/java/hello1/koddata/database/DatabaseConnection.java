package hello1.koddata.database;

import java.sql.ResultSet;

public interface DatabaseConnection {

    void connect();

    void close();

    ResultSet executeQuery(String query);
}
