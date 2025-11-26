package hello1.koddata.dataframe.loader;

import hello1.koddata.dataframe.Column;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public abstract class DataFrameLoader {

    protected DataFrame frame;
    protected Column[] columns;

    public abstract void load(InputStream in, int startRow, int endRow) throws IOException;

    public DataFrame getFrame() {
        return frame;
    }

    public List<Column> getColumns() {
        return List.of(columns);
    }
}
