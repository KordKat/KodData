package hello1.koddata.dataframe.loader;

import hello1.koddata.dataframe.Column;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public abstract class DataFrameLoader {

    protected Column[] columns;

    public abstract void load(InputStream in) throws IOException;


    public Column[] getColumns() {
            return columns;
    }
}
