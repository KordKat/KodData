package hello1.koddata.engine.function;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import hello1.koddata.database.CqlDatabaseConnection;
import hello1.koddata.database.DatabaseConnection;
import hello1.koddata.database.RelationalDatabaseConnection;
import hello1.koddata.engine.Value;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;

public class ConnectionFunction extends KodFunction<CompletableFuture<DatabaseConnection>>{
    @Override
    public Value<CompletableFuture<DatabaseConnection>> execute() throws KException {
        if (!arguments.containsKey("databaseType")){
            throw new KException(ExceptionCode.KDE0012,"Function connection need argument databaseType");
        }if (!arguments.containsKey("databaseName")){
            throw new KException(ExceptionCode.KDE0012,"Function connection need argument databaseName");
        }
        if (!arguments.containsKey("host")){
            throw new KException(ExceptionCode.KDE0012,"Function connection need argument host");
        }
        if (!arguments.containsKey("port")){
            throw new KException(ExceptionCode.KDE0012,"Function connection need argument port");
        }


        Value<?> databaseType = arguments.get("databaseType");
        Value<?> databaseName = arguments.get("databaseName");
        Value<?> host = arguments.get("host");
        Value<?> port = arguments.get("port");

        if (!(databaseType.get() instanceof String databaseTypeString)) {
            throw new KException(ExceptionCode.KDE0012, "databaseType should be string");
        }
        if (!(databaseName.get() instanceof String dbName)) {
            throw new KException(ExceptionCode.KDE0012, "databaseName should be string");
        }
        if (!(host.get() instanceof String hostString)) {
            throw new KException(ExceptionCode.KDE0012, "host should be string");
        }
        if (!(port.get() instanceof Integer portInt)) {
            throw new KException(ExceptionCode.KDE0012, "port should be integer");
        }
        if (databaseType.get().equals("cassandra")){
            if (!arguments.containsKey("dataCentre")){
                throw new KException(ExceptionCode.KDE0012,"cassandra connection need argument dataCentre");
            }
            else {
                Value<?> dataCentre = arguments.get("dataCentre");
                if (!(dataCentre.get() instanceof String dataCentreString)) {
                    throw new KException(ExceptionCode.KDE0012, "host should be string");
                }
                else {
                    return new Value<>(CompletableFuture.supplyAsync(() ->{
                        CqlDatabaseConnection cqlDatabaseConnection = new CqlDatabaseConnection(CqlSession.builder().addContactPoint(new InetSocketAddress(hostString , portInt)).withLocalDatacenter(dataCentreString).withKeyspace(CqlIdentifier.fromCql(dbName)).build());
                        cqlDatabaseConnection.connect();
                        return cqlDatabaseConnection;
                    }));
                }

            }

        }
        else {
            if (!arguments.containsKey("user")){
                throw new KException(ExceptionCode.KDE0012,"Relational Database connection need argument user");
            }
            if (!arguments.containsKey("pass")){
                throw new KException(ExceptionCode.KDE0012,"Relational Database connection need argument pass");
            }
            else{
                Value<?> user = arguments.get("user");
                Value<?> pass = arguments.get("pass");
                if (!(user.get() instanceof String userString)) {
                    throw new KException(ExceptionCode.KDE0012, "user should be string");
                }
                if (!(pass.get() instanceof String passString)) {
                    throw new KException(ExceptionCode.KDE0012, "pass should be string");
                }
                return new Value<>(CompletableFuture.supplyAsync(() ->{
                    RelationalDatabaseConnection relationalDatabaseConnection = new RelationalDatabaseConnection(hostString , portInt ,userString , passString , dbName , databaseTypeString);
                    relationalDatabaseConnection.connect();
                    return relationalDatabaseConnection;
                }));
            }

        }
    }
}
