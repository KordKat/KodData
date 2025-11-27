package hello1.koddata.engine;

import hello1.koddata.dataframe.Column;
import hello1.koddata.dataframe.DataFrameCursor;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DistinctOperation implements ColumnOperation {

    @Override
    public Value<?> operate(Value<?> value) throws KException {

        // ตรวจว่าเป็น Column
        if (!(value.get() instanceof Column column)) {
            throw new KException(ExceptionCode.KD00005, "Only column is accept");
        }

        int rows = Math.toIntExact(column.getMetaData().getRows());
        DataFrameCursor cursor = new DataFrameCursor();

        Set<Object> seen = new HashSet<>();
        List<Integer> indexList = new ArrayList<>();

        // ไล่เช็คค่าทีละแถว
        for (int i = 0; i < rows; i++) {

            Value<?> cell = column.readRow(i, cursor);
            Object item = (cell == null) ? null : cell.get();

            // ถ้าเจอครั้งแรก ให้เก็บ index ไว้
            if (seen.add(item)) {
                indexList.add(i);
            }
        }

        // คืนค่าเป็น List ของ index
        return new Value<>(indexList);
    }
}
