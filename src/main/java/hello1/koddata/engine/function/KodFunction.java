package hello1.koddata.engine.function;

import hello1.koddata.engine.Value;
import hello1.koddata.exception.KException;

import java.util.HashMap;
import java.util.Map;

// Abstract
//Strategy Pattern: สำหรับการเลือกใช้ algorithm ต่างๆ
public abstract class KodFunction <T>{
//    Encapsulation
    protected Map<String , Value<?>> arguments = new HashMap<>();


    public void addArgument(String name, Value<?> value){
        arguments.put(name , value);
    }
    // Abstract
    public abstract Value<T> execute() throws KException;
}
