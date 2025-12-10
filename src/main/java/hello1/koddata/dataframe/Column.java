package hello1.koddata.dataframe;
import hello1.koddata.engine.Value;
import hello1.koddata.exception.KException;
import java.util.List;
//import hello1.koddata.memory.Memory;
//import hello1.koddata.memory.MemoryGroup;
//import sun.misc.Unsafe;
//import hello1.koddata.concurrent.IdCounter;
//import hello1.koddata.engine.NullValue;
//import java.lang.reflect.Field;
//import java.nio.ByteBuffer;
//import java.sql.Timestamp;
//import java.util.ArrayList;
//import java.util.Date;
// import hello1.koddata.exception.ExceptionCode;
//import java.util.Set;

public class Column{
    private ColumnMetaData metaData;
    private List<Value<?>> column;
    public Column(){}

    public Column(String name, List<Value<?>> data, ColumnMetaData.ColumnDType dType) throws KException {
        setupMetadata(name, dType);
        this.column = data;
    }

    public void setupMetadata(String name, ColumnMetaData.ColumnDType dType){
        metaData = new ColumnMetaData(name, dType);

    }

    public ColumnMetaData getMetaData() {
        return metaData;
    }

    public Value<?> readRow(int index){
        return column.get(index);
    }
    public int size() {
        return column != null ? column.size() : 0;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append(metaData.getName())
                .append(" (")
                .append(metaData.getDType())
                .append(")")
                .append(":\n");

        for (Value<?> v : column) {
            sb.append("  ").append(v.toString()).append("\n");
        }

        return sb.toString();
    }
}