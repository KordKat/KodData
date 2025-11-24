package hello1.koddata.database;

import com.datastax.oss.driver.api.core.CqlSession;
import hello1.koddata.utils.Either;

import java.sql.ResultSet;

public class CqlDatabaseConnection implements DatabaseConnection {

    private final CqlSession session;

    public CqlDatabaseConnection(CqlSession session){
        this.session = session;
    }

    @Override
    public void connect() {
        if (session == null) {
            throw new RuntimeException("CqlSession is null.");
        }
    }

    @Override
    public void close() {
        try {
            if (session != null) session.close();
        } catch (Exception ignored) {}
    }

    @Override
    public Either<ResultSet, com.datastax.oss.driver.api.core.cql.ResultSet> executeQuery(String query) {
        try {
            if (session == null) connect();

            assert session != null;
            com.datastax.oss.driver.api.core.cql.ResultSet cqlRs = session.execute(query);

            return Either.right(cqlRs);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}