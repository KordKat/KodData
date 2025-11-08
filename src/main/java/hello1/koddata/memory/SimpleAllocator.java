package hello1.koddata.memory;

import hello1.koddata.utils.ref.Reference;
import hello1.koddata.utils.ref.UniqueReference;

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
