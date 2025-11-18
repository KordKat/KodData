package hello1.koddata.engine.function;

import hello1.koddata.engine.QueryOperation;
import hello1.koddata.engine.Value;
import hello1.koddata.exception.KException;

import java.util.HashMap;
import java.util.Map;

public abstract class KodFunction <T>{
    protected Map<String , Value<?>> arguments = new HashMap<>();


    public void addArgument(String name, Value<?> value){
        arguments.put(name , value);
    }
    public abstract Value<T> execute() throws KException;
}
