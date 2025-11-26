package hello1.koddata.engine;

import java.util.List;

public class ATan2Operation implements QueryOperation {

    @Override
    public Value<?> operate(Value<?> value) {
        if (value == null || value.get() == null)
            return value;

        Object v = value.get();

        // List case: [y, x]
        if (v instanceof List<?> list) {
            if (list.size() >= 2 &&
                    list.get(0) instanceof Number y &&
                    list.get(1) instanceof Number x) {

                double result = Math.atan2(y.doubleValue(), x.doubleValue());
                return new Value<>(result);
            }
            return value; // type ไม่ตรง ไม่แตะ ไม่ยุ่ง
        }

        // Array case: Number[]{ y, x }
        if (v instanceof Number[] arr) {
            if (arr.length >= 2 &&
                    arr[0] instanceof Number y &&
                    arr[1] instanceof Number x) {

                double result = Math.atan2(y.doubleValue(), x.doubleValue());
                return new Value<>(result);
            }
            return value;
        }

        return value;
    }
}

