package hello1.koddata.utils.collection.primitive.lists;

import hello1.koddata.memory.SafeMemory;
import hello1.koddata.utils.math.MathUtils;

import java.nio.ByteBuffer;

public class OffHeapFloatList extends FloatList {

    private SafeMemory safeMemory;

    private static final int DEFAULT_CAPACITY = 8;
    private int size = 0;

    public OffHeapFloatList(){
        this(DEFAULT_CAPACITY);
    }

    public OffHeapFloatList(int capacity){
        safeMemory = SafeMemory.allocate(capacity * 2);
    }

    public void add(float i){
        ensureCapacity();
        safeMemory.setData(size * 4L, i);
        size++;
    }

    public void add(int index, float value) {
        if (index < 0 || index > size) {
            throw new IndexOutOfBoundsException("Index: " + index);
        }
        for (int i = size - 1; i >= index; i--) {
            float current = safeMemory.readFloat((long) i * 4);
            safeMemory.setData((long) (i + 1) * 4, current);
        }
        safeMemory.setData((long) index * 4, value);

        size++;
    }

    public void addAll(float[] array, int count) {
        if (array == null) {
            throw new NullPointerException("Array cannot be null");
        }
        if (count < 0 || count > array.length) {
            throw new IllegalArgumentException("Invalid count: " + count);
        }

        int requiredCapacity = size + count;
        ensureCapacity(requiredCapacity);

        long destOffset = (long) size * 2;

        byte[] bytes = new byte[count * 2];
        ByteBuffer.wrap(bytes).asFloatBuffer().put(array, 0, count);
        safeMemory.setData(destOffset, bytes);

        size += count;
    }

    public void addAll(float[] array){
        addAll(array, array.length);
    }

    public float get(int index){
        checkIndex(index);
        return  safeMemory.readFloat(index * 4L);
    }

    public void set(int index, float i){
        checkIndex(index);
        safeMemory.setData(index * 4L, i);
    }

    public float remove(int index) {
        checkIndex(index);

        long elementSize = 2L;

        float removed = safeMemory.readFloat(index * elementSize);

        for (int i = index; i < size - 1; i++) {
            float nextValue = safeMemory.readFloat((i + 1L) * elementSize);
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
            SafeMemory newMemory = SafeMemory.allocate((long) newCapacity * 2);
            byte[] oldBytes = safeMemory.readBytes(0, size * 4);
            newMemory.setData(0, oldBytes);

            safeMemory.free();
            safeMemory = newMemory;
        }
    }


    private void ensureCapacity() {
        ensureCapacity((int)(size + 1) * 2);
    }

    private void ensureCapacity(int requiredCapacity) {
        long capacity = safeMemory.size();
        if (requiredCapacity < capacity) {
            return;
        }

        long newCapacity = MathUtils.nearestPowerOf2(requiredCapacity * 2);
        SafeMemory newMemory = SafeMemory.allocate(newCapacity * 2);

        byte[] oldBytes = safeMemory.readBytes(0, size * 4);
        newMemory.setData(0, oldBytes);

        safeMemory.free();
        safeMemory = newMemory;
    }

    private void checkIndex(int index){
        if(index < 0 || index >= size){
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
        }
    }

    public void free(){
        safeMemory.free();
    }

}
