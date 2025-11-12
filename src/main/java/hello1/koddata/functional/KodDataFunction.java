package hello1.koddata.functional;

import hello1.koddata.engine.Value;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class KodDataFunction {

    private Map<String, Class<?>> arguments = new HashMap<>();
    private Consumer<Map<String, Value<?>>> function;

    public KodDataFunction addArgument(String name, Class<?> c){
        arguments.put(name, c);
        return this;
    }

    public KodDataFunction removeArguments(String...name){
        for(String n : name){
            arguments.remove(n);
        }
        return this;
    }

    public KodDataFunction setFunction(Consumer<Map<String, Value<?>>> function){
        this.function = function;
        return this;
    }

}
