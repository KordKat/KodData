package hello1.koddata.utils.collection.primitive.lists;

import hello1.koddata.memory.SafeMemory;
import hello1.koddata.utils.math.MathUtils;

import java.nio.ByteBuffer;

public class OffHeapByteList extends ByteList {

    private SafeMemory safeMemory;

    private static final int DEFAULT_CAPACITY = 8;
    private int size = 0;

    public OffHeapByteList(){
        this(DEFAULT_CAPACITY);
    }

    public OffHeapByteList(int capacity){
        safeMemory = SafeMemory.allocate(capacity);
    }

    public void add(int i){
        ensureCapacity();
        safeMemory.setData(size, i);
    }

    public void add(int index, byte value) {
        if (index < 0 || index > size) {
            throw new IndexOutOfBoundsException("Index: " + index);
        }
        for (int i = size - 1; i >= index; i--) {
            int current = safeMemory.readInt((long) i);
            safeMemory.setData((long) (i + 1) , current);
        }
        safeMemory.setData((long) index , value);

        size++;
    }

    public void addAll(byte[] array, int count) {
        if (array == null) {
            throw new NullPointerException("Array cannot be null");
        }
        if (count < 0 || count > array.length) {
            throw new IllegalArgumentException("Invalid count: " + count);
        }

        int requiredCapacity = size + count;
        ensureCapacity(requiredCapacity);

        long destOffset = (long) size;

        // Copy from int[] → native memory
        byte[] bytes = new byte[count];
        ByteBuffer.wrap(bytes).put(array, 0, count);
        safeMemory.setData(destOffset, bytes);

        size += count;
    }

    public void addAll(byte[] array){
        addAll(array, array.length);
    }

    public byte get(int index){
        checkIndex(index);
        return  safeMemory.readBytes(index , 1)[0];
    }

    public void set(int index, byte i){
        checkIndex(index);
        safeMemory.setData(index, i);
    }

    public byte remove(int index) {
        checkIndex(index);

        long elementSize = 1L; // each int = 2 bytes

        // Read removed value
        byte removed = safeMemory.readBytes(index * elementSize , 1)[0];
//.......................................
        // Shift remaining elements left by one
        for (int i = index; i < size - 1; i++) {
            byte nextValue = safeMemory.readBytes((i + 1L) * elementSize , 1)[0];
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
            SafeMemory newMemory = SafeMemory.allocate((long) newCapacity);
            byte[] oldBytes = safeMemory.readBytes(0, size);
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

        long newCapacity = MathUtils.nearestPowerOf2(requiredCapacity);
        SafeMemory newMemory = SafeMemory.allocate(newCapacity);

        byte[] oldBytes = safeMemory.readBytes(0, size);
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
