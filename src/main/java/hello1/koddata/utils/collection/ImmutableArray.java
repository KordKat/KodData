package hello1.koddata.utils.collection;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

//Inheritance
public final class ImmutableArray<T> implements Iterable<T> {

    private final T[] data;

    @SuppressWarnings("unchecked")
    public ImmutableArray(T[] data) {
        this.data = (T[]) new Object[data.length];
        System.arraycopy(data, 0, this.data, 0, data.length);
    }

    @SuppressWarnings("unchecked")
    public ImmutableArray(Collection<T> collection) {
        this.data = (T[]) new Object[collection.size()];
        T[] data = collection.toArray((T[]) new Object[collection.size()]);
        System.arraycopy(data, 0, this.data, 0, data.length);
    }

    public int length() {
        return data.length;
    }

    public T get(int index) {
        if (index < 0 || index >= data.length) {
            throw new IndexOutOfBoundsException("Index: " + index);
        }
        return data[index];
    }

    public T[] toArray() {
        return Arrays.copyOf(data, data.length);
    }

    //Polymorphism
    @Override
    public Iterator<T> iterator() {
        return Arrays.asList(data).iterator();
    }

    //Polymorphism
    @Override
    public String toString() {
        return Arrays.toString(data);
    }

}
