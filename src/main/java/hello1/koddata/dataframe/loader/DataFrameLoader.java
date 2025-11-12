package hello1.koddata.dataframe.loader;

import hello1.koddata.dataframe.Column;
import hello1.koddata.dataframe.DataFrame;

import java.io.InputStream;

public abstract class DataFrameLoader {

    protected DataFrame frame;
    protected Column[] columns;

    public abstract void load(InputStream in);

    public DataFrame getFrame() {
        return frame;
    }

    public Column[] getColumns() {
        return columns;
    }
}
