package hello1.koddata.engine.function;

import hello1.koddata.database.DatabaseConnection;
import hello1.koddata.dataframe.loader.CSVLoader;
import hello1.koddata.dataframe.loader.DataFrameLoader;
import hello1.koddata.dataframe.loader.DatabaseLoader;
import hello1.koddata.engine.DataSource;
import hello1.koddata.engine.Value;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class FetchFunction extends KodFunction<CompletableFuture<DataFrameLoader>>{
    @Override
    public Value<CompletableFuture<DataFrameLoader>> execute() throws KException {
        if (!arguments.containsKey("datatype")){
            throw new KException(ExceptionCode.KDE0012,"Function fetch need argument datatype");
        }
        if (!arguments.containsKey("datasource")){
            throw new KException(ExceptionCode.KDE0012,"Function fetch need argument datasource");
        }

        Value<?> value = arguments.get("datatype");
        Value<?> source = arguments.get("datasource");

        Value<DataSource> dataSourceValue = null;

        if (value.get() instanceof DataSource dataSource){
            dataSourceValue = new Value<>(dataSource);
        }
        else {
            throw new KException(ExceptionCode.KDE0012,"argument Data Source should be DATABASE , CSV");
        }
        final Value<DataSource> finalDataSource = dataSourceValue;
        if (dataSourceValue.get().equals(DataSource.CSV)){
            if (source.get() instanceof String s){
                final File file = new File(s);
                if (!file.exists()){
                    throw new KException(ExceptionCode.KDE0013,"There are no file in that name exists");
                }
                if (!arguments.containsKey("memoryGroupName")){
                    throw new KException(ExceptionCode.KDE0012,"You need to write memoryGroupName to use fetch function in csv or json");
                }
                Value<?> memoryGroupName = arguments.get("memoryGroupName");
                if (!(memoryGroupName.get() instanceof String memoryGroupNameString)) {
                    throw new KException(ExceptionCode.KDE0012, "memoryGroupName should be string");
                }
                return new Value<>(CompletableFuture.supplyAsync(() -> {
                    DataFrameLoader dataFrameLoader = null;
                    if (finalDataSource.get().equals(DataSource.CSV)){


                        dataFrameLoader = new CSVLoader(memoryGroupNameString);
                    }
                    try {
                        dataFrameLoader.load(new FileInputStream(file));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    return dataFrameLoader;
                }));
            }
            else {
                throw new KException(ExceptionCode.KDE0012,"source can only use string ");
            }

        }
        else {
            if(source.get() instanceof DatabaseConnection databaseConnection){
                if (!arguments.containsKey("memoryGroupName")){
                    throw new KException(ExceptionCode.KDE0012,"You need to write memoryGroupName to use fetch function in csv or json");
                }
                Value<?> memoryGroupName = arguments.get("memoryGroupName");
                if (!(memoryGroupName.get() instanceof String memoryGroupNameString)) {
                    throw new KException(ExceptionCode.KDE0012, "memoryGroupName should be string");
                }

                if(!arguments.containsKey("query")){
                    throw new KException(ExceptionCode.KDE0012,"Function fetch database need argument query");
                }


                Value<?> query = arguments.get("query");
                if (query.get() instanceof String queryString){
                    return new Value<>(CompletableFuture.supplyAsync(() -> {
                        DataFrameLoader dataFrameLoader;



                        dataFrameLoader = new DatabaseLoader(databaseConnection, queryString , memoryGroupNameString);
                        try {
                            dataFrameLoader.load(null);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        return dataFrameLoader;
                    }));
                }
                else {
                    throw new KException(ExceptionCode.KDE0012,"query should be string");
                }


            }
            else {
                throw new KException(ExceptionCode.KDE0012,"database can only use databaseConnection ");
            }
        }
    }

}
