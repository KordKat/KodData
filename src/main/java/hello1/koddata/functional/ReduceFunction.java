package hello1.koddata.functional;

import java.util.function.Function;

public class ReduceFunction<T> {

    private T startValue;
    private Function<T, T> fn;
    public ReduceFunction(T startValue, Function<T, T> fn) {}

}
