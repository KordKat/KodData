package hello1.koddata.dataframe;


public class ColumnMetaData {

    public enum ColumnDType {
        SCALAR_INT,
        SCALAR_DOUBLE,
        SCALAR_STRING,
        LIST_INT,
        LIST_DOUBLE,
        LIST_STRING,
        SCALAR_LOGICAL,
        SCALAR_DATE,
        SCALAR_TIMESTAMP,
        LIST_LOGICAL,
        LIST_DATE,
        LIST_TIMESTAMP;
    }
    // Encapsulation
    private final String name;
    private ColumnDType dType;

    public ColumnMetaData(String name, ColumnDType dType){
        this.name = name;
        this.dType = dType;
    }

    //Encapsulation
    public String getName() {
        return name;
    }

    //Encapsulation
    public ColumnDType getDType() {
        return dType;
    }
}
