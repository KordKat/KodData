package hello1.koddata.serialization;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class ObjectSerializer implements Serializer{
    @Override
    public byte[] serialize(Serializable serializable) {
        if (serializable == null) {
            System.out.println("serializable is null");
        }

        try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
             ObjectOutputStream objectOut = new ObjectOutputStream(byteOut)) {

            objectOut.writeObject(serializable);
            objectOut.flush();
            return byteOut.toByteArray();

        } catch (IOException e) {
            System.out.println("Error");
            throw new RuntimeException(e);
        }
    }
}

