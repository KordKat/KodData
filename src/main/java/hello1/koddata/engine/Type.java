package hello1.koddata.engine;

public class Type {
    private String typeName;
    private Class<?> clazz;


    public Type(String typeName , Class<?> clazz){
        this.typeName = typeName;
        this.clazz = clazz;
    }

    public String getTypeName() {
        return typeName;
    }
    public Class<?> getClazz() {
        return clazz;
    }
}
