package hello1.koddata.dataframe;

import hello1.koddata.engine.QueryExecution;
import hello1.koddata.engine.Value;
import hello1.koddata.functional.ReduceFunction;

import java.io.OutputStream;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

public class DataFrame {
    // perform only logical
    private DataFrameSchema schema;
    private QueryExecution queryExecution;

    public DataFrame(DataFrameSchema schema){}
    public DataFrame(){}

    public void writeTo(DataTransformer transformer, OutputStream os){}

    private void transformEveryRow(String format){}

    public byte[] show(int numRows){ return null; }

    public byte[] show(){ return show(20); }

    public DataFrame join(DataFrame right){ return null; }

    public DataFrame join(DataFrame right, String using, String joinType) { return null; }

    public DataFrame join(DataFrame right, String using) {return join(right, using, "inner"); }

    public DataFrame sort() { return null; }

    public DataFrame sort(String...columnOrder) {return null; }

    public DataFrame order(String...columnOrder) { return null; }

    public DataFrame select(String...columns) {return null; }

    public DataFrame where(String column, Predicate<? extends Value<?>> predicate) { return null; }

    public DataFrame filter(String column, Predicate<? extends Value<?>> predicate) {return null; }

    public DataFrame groupBy(String...columns) { return null; }

    public DataFrame aggregate(String fnName, String column) { return null; }

    public Value<?> reduce(String column, ReduceFunction<Value<?>> reducer) {return null;}

    public DataFrame drop(String...columns) {return null; }

    public DataFrame rename(String oldName, String newName) {return null; }

    public DataFrame distinct() {return null; }

    public DataFrame head(int n) { return null; }

    public DataFrame tail(int n) {return null; }

    public DataFrame sample(double fraction) {return null; }

    public DataFrame limit(int n) {return null; }

    public DataFrame dropNa(String...columns) {return null;}

    public DataFrame fillNa(String column, Value<?> replacement) {return null; }

    public DataFrame union(DataFrame other) {return null; }
    public DataFrame intersect(DataFrame other) {return null; }
    public DataFrame except(DataFrame other) {return null; }

}
