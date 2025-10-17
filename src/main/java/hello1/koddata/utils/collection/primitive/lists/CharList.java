package hello1.koddata.utils.collection.primitive.lists;

import hello1.koddata.utils.math.MathUtils;

public class CharList {

    private char[] data;
    private int size = 0;
    private static final int DEFAULT_CAPACITY = 8;

    public CharList() {
        data = new char[DEFAULT_CAPACITY];
    }

    public CharList(int capacity){
        if(capacity <= 0){
            throw new IllegalArgumentException("Capacity must be greater than zero");
        }

        data = new char[capacity];
    }

    public void add(char i){
        ensureCapacity();
        data[size++] = i;
    }

    public void add(int index, char i){
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

    public void addAll(char[] array, int count) {
        int requiredCapacity = size + count;
        if (requiredCapacity > data.length) {
            int newCapacity = MathUtils.nearestPowerOf2(requiredCapacity * 2);
            char[] newData = new char[newCapacity];
            System.arraycopy(data, 0, newData, 0, size);
            data = newData;
        }
        System.arraycopy(array, 0, data, size, count);
        size += count;
    }

    public void addAll(char[] array){
        addAll(array, array.length);
    }

    public char get(int index){
        checkIndex(index);
        return data[index];
    }

    public void set(int index, char i){
        checkIndex(index);
        data[index] = i;
    }

    public char remove(int index){
        checkIndex(index);
        char removed = data[index];
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
        data = new char[DEFAULT_CAPACITY];
        size = 0;
    }

    private void shrink() {
        int currentCapacity = data.length;
        if (currentCapacity <= DEFAULT_CAPACITY) return;
        if (size <= currentCapacity / 4) {
            int newCapacity = Math.max(DEFAULT_CAPACITY, currentCapacity / 2);
            char[] newData = new char[newCapacity];
            System.arraycopy(data, 0, newData, 0, size);
            data = newData;
        }
    }

    private void ensureCapacity() {
        if(size == data.length){
            int newCapacity = data.length * 2;
            char[] newInt = new char[newCapacity];
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
