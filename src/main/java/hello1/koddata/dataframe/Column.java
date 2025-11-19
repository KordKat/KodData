package hello1.koddata.dataframe;

import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import hello1.koddata.memory.Memory;
import hello1.koddata.memory.MemoryGroup;
import hello1.koddata.utils.KodResourceNaming;
import hello1.koddata.utils.Serializable;

import java.nio.ByteBuffer;

public class Column implements Serializable, KodResourceNaming {
    private String memoryGroupName;
    private ColumnMetaData metaData;
    private Memory memory;
    private int sizePerElement;
    private int startIdx;
    private int endIdx;
    public Column(String name, int sizePerElement, String memoryGroupName, ByteBuffer dataBuffer, boolean[] notNullFlags, int elementSize, int startIdx, int endIdx) throws KException {
        setupMetadata(name, sizePerElement, memoryGroupName, false);
        if(MemoryGroup.get(memoryGroupName) == null){
            throw new KException(ExceptionCode.KDM0007, "Memory group '" + memoryGroupName + "' does not exist.");
        }
        memory = MemoryGroup.get(memoryGroupName)
                .allocate(new FixedColumnAllocator(dataBuffer, notNullFlags, elementSize));
        this.startIdx = startIdx;
        this.endIdx = endIdx;
    }

    public Column(String name, int sizePerElement, String memoryGroupName, ByteBuffer dataBuffer, boolean[] notNullFlags, int elementSize) throws KException {
        this(name, sizePerElement, memoryGroupName, dataBuffer, notNullFlags, elementSize, 0, dataBuffer.position() / sizePerElement);
    }

    public void setupMetadata(String name, int sizePerElement, String memoryGroupName, boolean isVariable){
        metaData = new ColumnMetaData(name, isVariable);
        this.sizePerElement = sizePerElement;
        this.memoryGroupName = memoryGroupName;
    }

    public ColumnMetaData getMetaData() {
        return metaData;
    }

    public int getSizePerElement() {
        return sizePerElement;
    }

    @Override
    public String getResourceName() {

    }

    @Override
    public byte[] serialize() {

    }
}
