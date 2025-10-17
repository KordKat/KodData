package hello1.koddata.utils.collection.primitive.lists;

import hello1.koddata.utils.math.MathUtils;

public class ShortList {

    private short[] data;
    private int size = 0;
    private static final int DEFAULT_CAPACITY = 8;

    public ShortList() {
        data = new short[DEFAULT_CAPACITY];
    }

    public ShortList(int capacity){
        if(capacity <= 0){
            throw new IllegalArgumentException("Capacity must be greater than zero");
        }

        data = new short[capacity];
    }

    public void add(short i){
        ensureCapacity();
        data[size++] = i;
    }

    public void add(int index, short i){
        if(index < 0 || index > size){
            throw new IndexOutOfBoundsException("Index: " + index);
        }
        ensureCapacity();
        for(int i2 = size; i2 > index; i--){
            data[i2] = data[i2-1];
        }
        data[index] = i;
        size++;
    }

    public void addAll(short[] array, int count) {
        int requiredCapacity = size + count;
        if (requiredCapacity > data.length) {
            int newCapacity = MathUtils.nearestPowerOf2(requiredCapacity * 2);
            short[] newData = new short[newCapacity];
            System.arraycopy(data, 0, newData, 0, size);
            data = newData;
        }
        System.arraycopy(array, 0, data, size, count);
        size += count;
    }

    public void addAll(short[] array){
        addAll(array, array.length);
    }

    public short get(int index){
        checkIndex(index);
        return data[index];
    }

    public void set(int index, short i){
        checkIndex(index);
        data[index] = i;
    }

    public short remove(int index){
        checkIndex(index);
        short removed = data[index];
        for(int i = index; i < size - 1; i++){
            data[i] = data[i + 1];
        }
        size--;
        shrink();
        return removed;
    }

    public int size() {
        return size;
    }

    public int capacity() {
        return data.length;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public void removeAll(){
        data = new short[DEFAULT_CAPACITY];
        size = 0;
    }

    private void shrink() {
        int currentCapacity = data.length;
        if (currentCapacity <= DEFAULT_CAPACITY) return;
        if (size <= currentCapacity / 4) {
            int newCapacity = Math.max(DEFAULT_CAPACITY, currentCapacity / 2);
            short[] newData = new short[newCapacity];
            System.arraycopy(data, 0, newData, 0, size);
            data = newData;
        }
    }

    private void ensureCapacity() {
        if(size == data.length){
            int newCapacity = data.length * 2;
            short[] newInt = new short[newCapacity];
            System.arraycopy(data, 0, newInt, 0, data.length);
            data = newInt;
        }
    }
    private void checkIndex(int index){
        if(index < 0 || index >= size){
            throw new IndexOutOfBoundsException("Index: " + index + ", Siz: " + size);
        }
    }
}
