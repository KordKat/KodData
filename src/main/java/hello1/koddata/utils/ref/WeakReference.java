package hello1.koddata.utils.ref;

public class WeakReference<T> {
    private final java.lang.ref.WeakReference<Reference.ControlBlock<T>> weak;

    WeakReference(Reference.ControlBlock<T> ctrl) {
        this.weak = new java.lang.ref.WeakReference<>(ctrl);
    }

    public SharedReference<T> lock() {
        Reference.ControlBlock<T> ctrl = weak.get();
        if (ctrl != null && ctrl.tryRetain()) {
            return new SharedReference<>(ctrl);
        }
        return null;
    }

    public boolean alive() {
        Reference.ControlBlock<T> c = weak.get();
        return c != null && c.alive();
    }
}
