package hello1.koddata.engine;

import com.datastax.oss.driver.internal.core.util.Strings;

import java.sql.Timestamp;
import java.util.Date;

public class CastOperation implements QueryOperation{

    private Type type;

    public CastOperation(Type type){
        this.type = type;
    }

    @Override
    public Value<?> operate(Value<?> value) {
        if(value.get() instanceof Number n && type.getTypeName().equals("String")){
            return new Value<>(n.toString());
        }
        else if(value.get() instanceof Number n && type.getTypeName().equals("Logical")){
            return new Value<>(!n.equals(0)  ? Boolean.TRUE : Boolean.FALSE);
        }
        else if(value.get() instanceof String s && type.getTypeName().equals("Number")){
            return new Value<>(Double.parseDouble(s));
        }
        else if(value.get() instanceof String s && type.getTypeName().equals("Logical")){
            return new Value<>(!s.isEmpty()  ? Boolean.TRUE : Boolean.FALSE);
        }
        else if(value.get() instanceof Boolean b && type.getTypeName().equals("String")){
            return new Value<>(b.toString());
        }
        else if(value.get() instanceof Boolean b && type.getTypeName().equals("Number")){
            return new Value<>(b.equals(Boolean.TRUE) ? "TRUE": "FALSE");
        }
        else if(value.get() instanceof Date d && type.getTypeName().equals("String")){
            return new Value<>(d.toString());
        }
        else if(value.get() instanceof Timestamp ts && type.getTypeName().equals("Date")){
            return new Value<>((Date)ts);
        }
        else if(value.get() instanceof Timestamp ts && type.getTypeName().equals("String")){
            return new Value<>(ts.toString());
        }
        return new Value<>(type);
    }
}
