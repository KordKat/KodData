package hello1.koddata.utils.collection.primitive.lists;

import hello1.koddata.utils.math.MathUtils;

public class FloatList {

    private float[] data;
    private int size = 0;
    private static final int DEFAULT_CAPACITY = 8;

    public FloatList() {
        data = new float[DEFAULT_CAPACITY];
    }

    public FloatList(int capacity){
        if(capacity <= 0){
            throw new IllegalArgumentException("Capacity must be greater than zero");
        }

        data = new float[capacity];
    }

    public void add(float i){
        ensureCapacity();
        data[size++] = i;
    }

    public void add(int index, float i){
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

    public void addAll(float[] array, int count) {
        int requiredCapacity = size + count;
        if (requiredCapacity > data.length) {
            int newCapacity = MathUtils.nearestPowerOf2(requiredCapacity * 2);
            float[] newData = new float[newCapacity];
            System.arraycopy(data, 0, newData, 0, size);
            data = newData;
        }
        System.arraycopy(array, 0, data, size, count);
        size += count;
    }

    public void addAll(float[] array){
        addAll(array, array.length);
    }

    public float get(int index){
        checkIndex(index);
        return data[index];
    }

    public void set(int index, float i){
        checkIndex(index);
        data[index] = i;
    }

    public float remove(int index){
        checkIndex(index);
        float removed = data[index];
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
        data = new float[DEFAULT_CAPACITY];
        size = 0;
    }

    private void shrink() {
        int currentCapacity = data.length;
        if (currentCapacity <= DEFAULT_CAPACITY) return;
        if (size <= currentCapacity / 4) {
            int newCapacity = Math.max(DEFAULT_CAPACITY, currentCapacity / 2);
            float[] newData = new float[newCapacity];
            System.arraycopy(data, 0, newData, 0, size);
            data = newData;
        }
    }

    private void ensureCapacity() {
        if(size == data.length){
            int newCapacity = data.length * 2;
            float[] newInt = new float[newCapacity];
            System.arraycopy(data, 0, newInt, 0, data.length);
            data = newInt;
        }
    }
    private void checkIndex(int index){
        if(index < 0 || index >= size){
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
        }
    }
}
