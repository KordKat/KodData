package hello1.koddata.dataframe.loader;

import hello1.koddata.database.DatabaseConnection;

import java.io.InputStream;

public class DatabaseLoader extends DataFrameLoader {

    private DatabaseConnection connection;

    public DatabaseLoader(DatabaseConnection connection){}

    @Override
    public void load(InputStream in) {

    }
}
