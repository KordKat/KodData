package hello1.koddata.dataframe;

public class ColumnMetaData {

    public enum ColumnType {
        INT(4, false),
        LONG(8, false),
        DOUBLE(8, false),
        FLOAT(4, false),
        BOOLEAN(1, false),
        BYTE(1, false),
        VARCHAR(0, true),
        VARINT(0, true),
        BLOB(0, true);

        private final int size;
        private final boolean variable;

        ColumnType(int size, boolean variable) {
            this.size = size;
            this.variable = variable;
        }

        public boolean isVariable() { return variable; }
        public int size() { return size; }
    }

    private final String name;
    private final ColumnType type;
    private int rows;
    private boolean isSharded = false;
    public ColumnMetaData(String name, ColumnType type){
        this.name = name;
        this.type = type;
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

    public ColumnType getType() {
        return type;
    }
}
