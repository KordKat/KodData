package hello1.koddata.engine;

import hello1.koddata.exception.KException;

public interface QueryOperation {

    Value<?> operate(Value<?> value) throws KException;

}
