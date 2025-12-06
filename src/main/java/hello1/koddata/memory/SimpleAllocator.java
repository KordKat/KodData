package hello1.koddata.memory;

public class SimpleAllocator implements Allocator {

    private long size;

    public SimpleAllocator(long size){
        this.size = size;
    }

    @Override
    public Memory allocate() {
        return WritableMemory.allocate(size);
    }

    @Override
    public void deallocate(Memory mem) {
        mem.free();
    }
}
