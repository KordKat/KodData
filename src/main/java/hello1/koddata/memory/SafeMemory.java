package hello1.koddata.memory;

import hello1.koddata.utils.ref.Reference;
import hello1.koddata.utils.ref.SharedReference;

public class SafeMemory extends WritableMemory {

    private final SharedReference<?> ref;

    private SafeMemory(long peer, long size){
        super(peer, size);
        ref = new SharedReference<>(new FakeNull(), new MemoryTidy(peer));
    }

    private SafeMemory(SafeMemory copy){
        super(copy);
        ref = copy.ref.retain();
    }

    public SafeMemory share(){
        return new SafeMemory(this);
    }

    public void close(){
        ref.close();
        peer = 0;
    }

    public static SafeMemory allocate(long size){
        long peer = getPeer(size);
        return new SafeMemory(peer, size);
    }


    class MemoryTidy implements Reference.Sebastian {
        final long peer;

        public MemoryTidy(long peer){
            this.peer = peer;
        }

        @Override
        public void tidy() {
            if(peer != 0){
                free();
            }
        }
    }

    static class FakeNull {
        //nah what are you finding bro?
    }

}
