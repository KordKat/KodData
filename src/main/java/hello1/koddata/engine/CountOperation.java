package hello1.koddata.engine;

import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import java.util.List;

//Inheritance
public class CountOperation implements ColumnOperation {

    //Polymorphism
    @Override
    public Value<?> operate(Value<?> value) throws KException {

        if (!(value.get() instanceof List<?> column)) {
            throw new KException(ExceptionCode.KD00005, "Only list is accept");
        }

        long count = 0;

        for (Object o : column) {

            if (!(o instanceof Value<?> cell)) {
                continue;
            }

            if (cell instanceof NullValue) continue;

            count++;
        }

        return new Value<>(count);
    }
}
