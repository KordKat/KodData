package hello1.koddata.dataframe;

public class Column {

    private final ColumnMetaData metaData;

    public Column(String name, ColumnMetaData.ColumnType type){
        metaData = new ColumnMetaData(name, type);
    }

    public ColumnMetaData getMetaData() {
        return metaData;
    }
}
