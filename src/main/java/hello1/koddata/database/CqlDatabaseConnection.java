package hello1.koddata.database;

import com.datastax.oss.driver.api.core.CqlSession;
import hello1.koddata.utils.Either;

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
    public Either<ResultSet, com.datastax.oss.driver.api.core.cql.ResultSet> executeQuery(String query) {
        return null;
    }
}
