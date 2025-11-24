package hello1.koddata.database;

import hello1.koddata.utils.Either;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Locale;

public class RelationalDatabaseConnection implements DatabaseConnection {
    private String host, user, pass;
    private int port;
    private String databaseName;
    private String dbmsName;
    private Connection connection;

    public RelationalDatabaseConnection(String host, int port, String user, String pass, String databaseName, String dbmsName){
        this.host = host;
        this.user = user;
        this.pass = pass;
        this.port = port;
        this.databaseName = databaseName;
        this.dbmsName = dbmsName.toLowerCase(Locale.ROOT);

    }

    @Override
    public void connect() {
        if (!dbmsName.equals("mysql")) {
            throw new RuntimeException("Only MySQL supported.");
        }
            try {
                Class.forName("com.mysql.cj.jdbc.Driver");
                String url = "jdbc:mysql://" + host + ":" + port + "/" + databaseName;
                connection = DriverManager.getConnection(url, user, pass);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }



    @Override
    public Either<ResultSet, com.datastax.oss.driver.api.core.cql.ResultSet> executeQuery(String query) {
        try {
            if (connection == null) connect();
            Statement st = connection.createStatement();
            return Either.left(st.executeQuery(query));
            //          if Cassandra ===== return Either.right(st.executeQuery(query));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try { if (connection != null) connection.close(); }
        catch (Exception ignore) {}
    }
}