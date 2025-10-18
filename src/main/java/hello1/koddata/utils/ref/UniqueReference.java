package hello1.koddata.utils.ref;

public class UniqueReference<T> extends Reference<T> implements AutoCloseable {

    private boolean closed = false;

    public UniqueReference(T referent, Sebastian cleaner){
        super(new ControlBlock<>(referent, cleaner));
        registerLeakWatcher(this, referent.getClass().getSimpleName());
    }

    @Override
    public synchronized T get() {
        if(closed) throw new IllegalStateException("UniqueReference closed");
        return ctrl.get();
    }

    @Override
    public synchronized boolean isValid() {
        return !closed && ctrl.alive();
    }

    @Override
    public synchronized void close(){
        if(!closed){
            closed = true;
            ctrl.release();
        }
    }



}
