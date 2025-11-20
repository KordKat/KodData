package hello1.koddata.dataframe;

import hello1.koddata.concurrent.cluster.ClusterIdCounter;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import hello1.koddata.memory.Memory;
import hello1.koddata.memory.MemoryGroup;
import hello1.koddata.net.NodeStatus;
import hello1.koddata.utils.KodResourceNaming;
import hello1.koddata.utils.Serializable;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

public class Column implements Serializable, KodResourceNaming {
    private static ClusterIdCounter columnIdCounter;
    private long id;
    private String memoryGroupName;
    private ColumnMetaData metaData;
    private Memory memory;
    private int sizePerElement;
    private int startIdx;
    private int endIdx;


    public static void setupIdCounter(Set<NodeStatus> peers){
        columnIdCounter = ClusterIdCounter.getCounter(Column.class, peers);
    }

    public Column(String name, int sizePerElement, String memoryGroupName, ByteBuffer dataBuffer, boolean[] notNullFlags, int elementSize, int startIdx, int endIdx) throws KException {
        setupMetadata(name, sizePerElement, memoryGroupName, false);
        id = columnIdCounter.count();
        if(MemoryGroup.get(memoryGroupName) == null){
            throw new KException(ExceptionCode.KDM0007, "Memory group '" + memoryGroupName + "' does not exist.");
        }
        memory = MemoryGroup.get(memoryGroupName)
                .allocate(new FixedColumnAllocator(dataBuffer, notNullFlags, elementSize));
        this.startIdx = startIdx;
        this.endIdx = endIdx;
    }

    public Column(String name, List<VariableElement> values, String memoryGroupName, boolean[] notNullFlags, int startIdx, int endIdx) throws KException {
        setupMetadata(name, sizePerElement, memoryGroupName, true);
        id = columnIdCounter.count();

        memory = MemoryGroup.get(memoryGroupName)
                .allocate(new VariableColumnAllocator(values, notNullFlags));

        this.startIdx = startIdx;
        this.endIdx = endIdx;
    }

    public Column(String name, String memoryGroupName, List<List<byte[]>> lists, List<boolean[]> perListNotNullFlags,
                  boolean[] columnNotNullFlags, int elementSize, int startIdx, int endIdx) throws KException {
        setupMetadata(name, elementSize, memoryGroupName, true);
        id = columnIdCounter.count();

        memory = MemoryGroup.get(memoryGroupName)
                .allocate(new FixedListColumnAllocator(lists,perListNotNullFlags,columnNotNullFlags,elementSize));
        this.startIdx = startIdx;
        this.endIdx = endIdx;
    }

    public Column(String name , String memoryGroupName,List<List<VariableElement>> lists,
                  List<boolean[]> perListNotNullFlags,
                  boolean[] columnNotNullFlags, int startIdx, int endIdx) throws KException {
        setupMetadata(name, -1, memoryGroupName, true);
        id = columnIdCounter.count();
        memory = MemoryGroup.get(memoryGroupName)
                .allocate(new VariableListColumnAllocator(lists,perListNotNullFlags,columnNotNullFlags));
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
        return String.format("%s::%d::%s", Column.class.getName(), id, getMetaData().getName());
    }

    @Override
    public byte[] serialize() {
        return null;
    }
}