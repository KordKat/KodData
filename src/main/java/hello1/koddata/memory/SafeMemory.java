package hello1.koddata.memory;

import hello1.koddata.concurrent.ref.Reference;
import hello1.koddata.concurrent.ref.SharedReference;

public class SafeMemory extends WritableMemory {

    private final Reference<?> ref;

    SafeMemory(long peer, long size){
        super(peer, size);
        ref = new SharedReference<>(null, new MemoryTidy(peer, size));
    }

    public static SafeMemory allocate(long size){
        long peer = getPeer(size);
        return new SafeMemory(peer, size);
    }


    class MemoryTidy implements Reference.Sebastian {
        final long peer;
        final long size;

        public MemoryTidy(long peer, long size){
            this.peer = peer;
            this.size = size;
        }

        @Override
        public void tidy() {
            if(peer != 0){
                free();
            }
        }
    }

}
