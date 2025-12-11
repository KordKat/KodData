package hello1.koddata.engine;

//Inheritance
//Strategy Pattern: สำหรับการเลือกใช้ algorithm ต่างๆ
public class IndexOfOperation implements QueryOperation {

    private final String search;

    public IndexOfOperation(String search){
        this.search = search;
    }

    //Polymorphism
    @Override
    public Value<?> operate(Value<?> value){
        if (value == null || value.get() == null) return value;

        if (value.get() instanceof String s)
            return new Value<>(s.indexOf(search));

        return value;
    }
}
