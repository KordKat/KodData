package hello1.koddata.sessions;

import hello1.koddata.dataframe.Column;
import hello1.koddata.dataframe.loader.DataFrameLoader;
import hello1.koddata.engine.Value;
import hello1.koddata.memory.MemoryGroup;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SessionData {
    private String sessionName;
    private Map<String, Value<?>> variables = new ConcurrentHashMap<>();

    private Map<String, Column[]> sessionDataFrame = new ConcurrentHashMap<>();

    private Map<Integer, String> preparedBlock = new ConcurrentHashMap<>();

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

    public void newDataFrame(DataFrameLoader loader){
        String name = loader.getFrame().getName();
        Column[] columns = loader.getColumns();

        sessionDataFrame.put(name, columns);

    }

    public void deallocDataFrame(String dfName){
        Column[] columns = sessionDataFrame.get(dfName);
        for(Column column : columns){
            //todo deallocate column
        }
    }

    public Map<Integer, String> getPreparedBlock() {
        return preparedBlock;
    }

    public void assignVariable(String name, Object value) {

    }
}
