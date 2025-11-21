package hello1.koddata.engine.function;

import hello1.koddata.dataframe.loader.DataFrameLoader;
import hello1.koddata.engine.DataName;
import hello1.koddata.engine.DataSource;
import hello1.koddata.engine.Value;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;

import java.util.concurrent.CompletableFuture;

public class ExportFunction extends KodFunction<CompletableFuture<DataFrameLoader>>{

    @Override
    public Value<CompletableFuture<DataFrameLoader>> execute() throws KException {
//        if (!arguments.containsKey("dataName")){
//            throw new KException(ExceptionCode.KDE0012,"Remove function need argument databaseName");
//        }
//        Value<?> dataName = arguments.get("dataName");
//        if (!(dataName.get() instanceof DataName dataNameDN)) {
//            throw new KException(ExceptionCode.KDE0012, "databaseName should be dataName");
//        }

        if (!arguments.containsKey("dataType")){
            throw new KException(ExceptionCode.KDE0012,"Remove function need argument databaseName");
        }
        Value<?> dataType = arguments.get("dataType");
        if (!(dataType.get() instanceof DataSource dataTypeDS)) {
            throw new KException(ExceptionCode.KDE0012, "dataType should be dataName");
        }
        return null;
    }
}
