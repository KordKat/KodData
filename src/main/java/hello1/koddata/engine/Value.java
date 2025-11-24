package hello1.koddata.engine;

import java.util.Comparator;

public class Value<T> {

    private T value;

    public Value(T value){
        this.value = value;
    }


    public T get(){
        return get(null);
    }

    public T get(T defaultValue){
        if(value == null) return defaultValue;
        return value;
    }

    public void set(T value){
        this.value = value;
    }

    @Override
    public String toString() {
        return value.toString();
    }
}
