package hello1.koddata.serialization;

import java.io.*;

public class ObjectSerializer implements Serializer{
    @Override
    public byte[] serialize(Serializable serializable) {
        if (serializable == null) {
            throw new IllegalArgumentException("Object to serialize is null");
        }

        try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
             ObjectOutputStream objectStream = new ObjectOutputStream(byteStream)) {
            objectStream.writeObject(serializable);
            objectStream.flush();
            return byteStream.toByteArray();

        } catch (IOException e) {
            throw new RuntimeException("Serialization failed", e);
        }
    }

    public Serializable deserialize(byte[] data) {
        if (data == null || data.length == 0) {
            throw new IllegalArgumentException("Byte array is empty");
        }

        try (ByteArrayInputStream byteIn = new ByteArrayInputStream(data);
             ObjectInputStream objectIn = new ObjectInputStream(byteIn)) {

            return (Serializable) objectIn.readObject();

        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Deserialization failed", e);
        }

    }

}
