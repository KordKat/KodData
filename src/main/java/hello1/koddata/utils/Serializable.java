package hello1.koddata.utils;

import hello1.koddata.exception.KException;

import java.util.HashMap;
import java.util.Map;

public interface Serializable {

    static final Map<Integer, Class<? extends Serializable>> wire = new HashMap<>();
    static final Map<Class<? extends Serializable>, Integer> wireToId = new HashMap<>();

    static void addWire(Integer wireId, Class<? extends Serializable> wireClass){
        wire.putIfAbsent(wireId, wireClass);
        wireToId.putIfAbsent(wireClass, wireId);
    }

    static Class<? extends Serializable> searchWireClass(Integer wireId){
        return wire.get(wireId);
    }

    static Integer searchWireId(Class<? extends Serializable> clazz){
        return wireToId.get(clazz);
    }

    byte[] serialize() throws KException;

    void deserialize(byte[] b);

}
