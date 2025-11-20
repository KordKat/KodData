package hello1.koddata.concurrent.cluster;

import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import hello1.koddata.utils.Serializable;

import java.io.*;
import java.util.HashMap;

public class ConsistentCriteria implements Serializable {

    private HashMap<String, Object> values = new HashMap<>();

    public ConsistentCriteria addCriteria(String name, Object value){
        values.put(name, value);
        return this;
    }

    public ConsistentCriteria removeCriteria(String name){
        values.remove(name);
        return this;
    }

    public Object get(String name){
        return values.get(name);
    }

    public boolean isConsistent(ConsistentCriteria criteria){
        boolean eq = true;
        for (String key : values.keySet()){
            Object t = values.get(key);
            Object o = criteria.get(key);
            if(t instanceof ConsistentCriteria ao && o instanceof ConsistentCriteria oo){
                eq = ao.isConsistent(oo);
                if(!eq) break;
            }else if(!t.equals(o)){
                eq = false;
                break;
            }
        }
        return eq;
    }

    @Override
    public byte[] serialize() throws KException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {

            oos.writeObject(values);
            oos.flush();
            return bos.toByteArray();

        } catch (IOException e) {
            throw new KException(ExceptionCode.KD00010, "Serialization failed: " + e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void deserialize(byte[] b) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(b);
             ObjectInputStream ois = new ObjectInputStream(bis)) {

            Object obj = ois.readObject();
            if (obj instanceof HashMap) {
                this.values = (HashMap<String, Object>) obj;
            } else {
                throw new RuntimeException("Deserialized object is not a HashMap");
            }

        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Deserialization failed", e);
        }
    }
}
