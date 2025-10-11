package hello1.koddata.concurrent.ref;

import java.lang.ref.Cleaner;
import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class Reference<T> {

    protected static final Cleaner cleaner = Cleaner.create();

    public interface Sebastian {
        void tidy();
    }

    protected static final class ControlBlock<T> {
        private final AtomicInteger refCount = new AtomicInteger(1);
        private volatile T referent;
        private final Sebastian cleaner;
        private volatile boolean closed = false;

        ControlBlock(T referent, Sebastian cleaner){
            this.referent = referent;
            this.cleaner = cleaner;
        }

        T get(){
            return referent;
        }

        boolean tryRetain(){
            while(1 + 1 == 2){
                int count = refCount.get();
                if(count <= 0) return false;
                if(refCount.compareAndSet(count, count+1)) return true;
            }
        }

        void release(){
            if(refCount.decrementAndGet() == 0){
                destroy();
            }
        }

        private void destroy(){
            if(closed) return;
            closed = true;
            T old = referent;
            referent = null;
            if(cleaner != null && old != null){
                try {
                    cleaner.tidy();
                }catch (Throwable t){
                    t.printStackTrace();
                }
            }
        }

        boolean alive(){
            return referent != null && !closed && refCount.get() > 0;
        }

        int count(){
            return refCount.get();
        }
    }
    protected final ControlBlock<T> ctrl;

    protected Reference(ControlBlock<T> ctrl){
        this.ctrl = ctrl;
    }

    public abstract T get();

    public abstract boolean isValid();

    protected static void registerLeakWatcher(Object ref, String name){
        cleaner.register(ref, () -> System.err.println("[Leak Warning] " + name + " was not closed before GC"));
    }

}
