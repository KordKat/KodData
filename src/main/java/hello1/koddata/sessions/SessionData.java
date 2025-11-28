package hello1.koddata.sessions;

import hello1.koddata.dataframe.Column;
import hello1.koddata.dataframe.ColumnArray;
import hello1.koddata.dataframe.loader.DataFrameLoader;
import hello1.koddata.engine.DataName;
import hello1.koddata.engine.Value;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import hello1.koddata.memory.MemoryGroup;
import hello1.koddata.utils.collection.ImmutableArray;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SessionData {
    private String sessionName;
    private Map<String, Value<?>> variables = new ConcurrentHashMap<>();

    private Map<String, ColumnArray> sessionDataFrame = new ConcurrentHashMap<>();

    private MemoryGroup memoryGroup;

    public SessionData(String sessionName){
        this.sessionName = sessionName;
    }

    public Value<?> get(String varName){
        return variables.getOrDefault(varName, null);
    }

    public void set(String varName, Value<?> value){
        this.variables.put(varName, value);
    }


    public void deallocDataFrame(String dfName){
        ColumnArray columnArray = sessionDataFrame.get(dfName);
        if(columnArray == null) return;
        columnArray.deallocate();
        sessionDataFrame.remove(dfName);
    }

    public void assignVariable(DataName name, Object value) throws KException {
        if(value instanceof Value<?> val){
            value = val.get();
        }
        if(value instanceof Column c){
            if(name.getIndex() == null){
                throw new KException(ExceptionCode.KDE00015, "Column name missing");
            }

            if(!sessionDataFrame.containsKey(name.getName())){
                sessionDataFrame.put(name.getName(), new ColumnArray(new ImmutableArray<>(new Column[]{c}), memoryGroup));
            }else {
                ColumnArray columnArray = sessionDataFrame.get(name.getName());
                if(columnArray == null) sessionDataFrame.put(name.getName(), new ColumnArray(new ImmutableArray<>(new Column[]{c}), memoryGroup));
                else columnArray.addColumn(c);
            }

        }else if(value instanceof ColumnArray columnArray){
            if(!sessionDataFrame.containsKey(name.getName())){
                variables.remove(name.getName());
                sessionDataFrame.put(name.getName(), columnArray);
            }else {
                ColumnArray old = sessionDataFrame.get(name.getName());
                if(old == null) sessionDataFrame.put(name.getName(), columnArray);
                else {
                    old.deallocate();
                    sessionDataFrame.put(name.getName(), columnArray);
                    variables.remove(name.getName());
                }
            }
        }else {
            variables.put(name.getName(), new Value<>(value));
            if(sessionDataFrame.containsKey(name.getName())){
                ColumnArray old = sessionDataFrame.get(name.getName());
                if(old != null) {
                    old.deallocate();
                    sessionDataFrame.remove(name.getName());

                }
            }
        }
    }

    public String getSessionName() {
        return sessionName;
    }

    public Map<String, ColumnArray> getSessionDataFrame() {
        return sessionDataFrame;
    }

    public Map<String, Value<?>> getVariables() {
        return variables;
    }

    public MemoryGroup getMemoryGroup() {
        return memoryGroup;
    }
}
