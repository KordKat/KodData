package hello1.koddata.dataframe;

import hello1.koddata.utils.SerialVersionId;

public class ColumnMetaData {

    private final String name;
    private boolean isVariable;
    private int rows;
    private boolean isSharded = false;
    private long serialVersionId = SerialVersionId.get;
    public ColumnMetaData(String name, boolean isVariable){
        this.name = name;
        this.isVariable = isVariable;
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
}
