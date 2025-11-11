package hello1.koddata.functional;

import hello1.koddata.engine.Value;

public interface KodDataFunction<A, B> {

    Value<B> apply(Value<A> v);

}
