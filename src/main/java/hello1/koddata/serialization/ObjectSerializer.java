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
            return new byte[0];
        }

        try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
             ObjectOutputStream objectStream = new ObjectOutputStream(byteStream)) {
            objectStream.writeObject(serializable);
            objectStream.flush();
            return byteStream.toByteArray();

        } catch (IOException e) {
            System.out.println("error");
            return new byte[0];
        }
    }

}
