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
    private boolean isVariable;
    private int rows;
    private boolean isSharded = false;
    private ColumnDType dType;

    public ColumnMetaData(String name, ColumnDType dType){
        this.name = name;
        this.dType = dType;
    }

    public void setSharded(boolean sharded) {
        isSharded = sharded;
    }

    public boolean isSharded() {
        return isSharded;
    }

    void setRows(int rows) {
        this.rows = rows;
    }

    public int getRows() {
        return rows;
    }

    public String getName() {
        return name;
    }

    public boolean isVariable() {
        return isVariable;
    }

    public ColumnDType getDType() {
        return dType;
    }
}
