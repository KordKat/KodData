package hello1.koddata.utils.ref;

//Inheritance
public class SharedReference<T> extends Reference<T> implements AutoCloseable {

    SharedReference(ControlBlock<T> ctrl) {
        super(ctrl);
        registerLeakWatcher(this, ctrl.get() != null ? ctrl.get().getClass().getSimpleName() : "Unknown");
    }

    public SharedReference(T obj, Sebastian cleaner) {
        super(new ControlBlock<>(obj, cleaner));
        registerLeakWatcher(this, obj.getClass().getSimpleName());
    }

    //Polymorphism
    @Override
    public T get() {
        return ctrl.get();
    }

    //Polymorphism
    @Override
    public boolean isValid() {
        return ctrl.alive();
    }

    public SharedReference<T> retain() {
        if (!ctrl.tryRetain()) throw new IllegalStateException("Object already destroyed");
        return new SharedReference<>(ctrl);
    }

    //Polymorphism
    @Override
    public void close() {
        ctrl.release();
    }

    public int useCount() {
        return ctrl.count();
    }

}
