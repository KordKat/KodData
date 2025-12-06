package hello1.koddata.engine;

public class FactorialOperation implements QueryOperation {

    private long fact(long n){
        long r = 1;
        for (long i = 2; i <= n; i++) r *= i;
        return r;
    }

    @Override
    public Value<?> operate(Value<?> value) {
        if (value == null || value.get() == null) return value;
        if (!(value.get() instanceof Number n)) return value;

        return new Value<>(fact(n.longValue()));
    }
}
