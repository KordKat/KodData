package hello1.koddata.database;

import hello1.koddata.utils.Either;

import java.sql.ResultSet;
// Abstract
public interface DatabaseConnection {

    void connect();

    void close();

    Either<ResultSet, com.datastax.oss.driver.api.core.cql.ResultSet> executeQuery(String query);
}
