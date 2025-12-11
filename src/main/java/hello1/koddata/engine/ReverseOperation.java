package hello1.koddata.engine;

//Inheritance
//Strategy Pattern: สำหรับการเลือกใช้ algorithm ต่างๆ

public class ReverseOperation implements QueryOperation {

    //Polymorphism
    @Override
    public Value<?> operate(Value<?> value){
        if (value == null || value.get() == null) return value;

        if (value.get() instanceof String s)
            return new Value<>(new StringBuilder(s).reverse().toString());

        return value;
    }
}
