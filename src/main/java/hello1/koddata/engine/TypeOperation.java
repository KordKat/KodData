package hello1.koddata.engine;

import java.sql.Timestamp;
import java.util.Date;
import java.util.List;

public class TypeOperation implements QueryOperation{

    @Override
    public Value<?> operate(Value<?> value) {
        if(value.get() instanceof List){
            return new Value<>(new Type("List" , List.class));
        }
        else if (value.get() instanceof Number){
            return new Value<>(new Type("Number" , Number.class));
        }
        else if (value.get() instanceof String){
            return new Value<>(new Type("String" , String.class));
        }
        else if (value.get() instanceof Boolean){
            return new Value<>(new Type("Logical" , Boolean.class));
        }
        else if (value.get() instanceof Date){
            return new Value<>(new Type("Date" , Date.class));
        }
        else if (value.get() instanceof Timestamp){
            return new Value<>(new Type("Timestamp" , Timestamp.class));
        }
        else {
            return new Value<>(new Type("Object" , Object.class));
        }
    }
}
