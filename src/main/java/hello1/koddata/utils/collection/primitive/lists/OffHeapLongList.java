package hello1.koddata.utils.collection.primitive.lists;

import hello1.koddata.memory.SafeMemory;
import hello1.koddata.utils.math.MathUtils;

import java.nio.ByteBuffer;

public class OffHeapLongList extends LongList {

    private SafeMemory safeMemory;

    private static final int DEFAULT_CAPACITY = 8;
    private int size = 0;

    public OffHeapLongList(){
        this(DEFAULT_CAPACITY);
    }

    public OffHeapLongList(int capacity){
        safeMemory = SafeMemory.allocate(capacity * 8);
    }

    public void add(int i){
        ensureCapacity();
        safeMemory.setData(size * 8L, i);
    }

    public void add(int index, long value) {
        if (index < 0 || index > size) {
            throw new IndexOutOfBoundsException("Index: " + index);
        }
        for (int i = size - 1; i >= index; i--) {
            int current = safeMemory.readInt((long) i * 8);
            safeMemory.setData((long) (i + 1) * 8, current);
        }
        safeMemory.setData((long) index * 8, value);

        size++;
    }

    public void addAll(long[] array, int count) {
        if (array == null) {
            throw new NullPointerException("Array cannot be null");
        }
        if (count < 0 || count > array.length) {
            throw new IllegalArgumentException("Invalid count: " + count);
        }

        int requiredCapacity = size + count;
        ensureCapacity(requiredCapacity);

        long destOffset = (long) size * 8;

        // Copy from int[] → native memory
        byte[] bytes = new byte[count * 8];
        ByteBuffer.wrap(bytes).asLongBuffer().put(array, 0, count);
        safeMemory.setData(destOffset, bytes);

        size += count;
    }

    public void addAll(long[] array){
        addAll(array, array.length);
    }

    public long get(int index){
        checkIndex(index);
        return  safeMemory.readLong(index * 8L);
    }

    public void set(int index, long i){
        checkIndex(index);
        safeMemory.setData(index * 8L, i);
    }

    public long remove(int index) {
        checkIndex(index);

        long elementSize = 8L; // each int = 8 bytes

        // Read removed value
        long removed = safeMemory.readLong(index * elementSize);

        // Shift remaining elements left by one
        for (int i = index; i < size - 1; i++) {
            long nextValue = safeMemory.readLong((i + 1L) * elementSize);
            safeMemory.setData(i * elementSize, nextValue);
        }

        size--;
        shrink();

        return removed;
    }

    public int size() {
        return size;
    }

    public int capacity() {
        return (int) safeMemory.size();
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public void removeAll(){
        safeMemory.resize(DEFAULT_CAPACITY);
        safeMemory.setData(0);
        size = 0;
    }

    private void shrink() {
        long currentCapacity = safeMemory.size();
        if (currentCapacity <= DEFAULT_CAPACITY) return;

        if (size <= currentCapacity / 4) {
            int newCapacity = Math.max(DEFAULT_CAPACITY, (int)currentCapacity / 2);
            SafeMemory newMemory = SafeMemory.allocate((long) newCapacity * 8);
            byte[] oldBytes = safeMemory.readBytes(0, size * 8);
            newMemory.setData(0, oldBytes);

            safeMemory.free();
            safeMemory = newMemory;
        }
    }


    private void ensureCapacity() {
        ensureCapacity((int)safeMemory.size());
    }

    private void ensureCapacity(int requiredCapacity) {
        long capacity = safeMemory.size();
        if (requiredCapacity <= capacity) {
            return;
        }

        long newCapacity = MathUtils.nearestPowerOf2(requiredCapacity * 8);
        SafeMemory newMemory = SafeMemory.allocate(newCapacity * 8);

        byte[] oldBytes = safeMemory.readBytes(0, size * 8);
        newMemory.setData(0, oldBytes);

        safeMemory.free();
        safeMemory = newMemory;
    }

    private void checkIndex(int index){
        if(index < 0 || index >= size){
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
        }
    }

}
