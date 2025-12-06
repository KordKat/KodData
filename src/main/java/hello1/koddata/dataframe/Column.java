package hello1.koddata.dataframe;

import hello1.koddata.concurrent.IdCounter;
import hello1.koddata.engine.NullValue;
import hello1.koddata.engine.Value;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import hello1.koddata.memory.Memory;
import hello1.koddata.memory.MemoryGroup;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

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

}
