package hello1.koddata.engine;

import hello1.koddata.net.UserClient;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class PrintOperation implements QueryOperation{
    private UserClient client;
    public PrintOperation(UserClient client){
        this.client = client;
    }

    @Override
    public Value<?> operate(Value<?> value) {

        client.write(ByteBuffer.wrap(value.get().toString().getBytes(StandardCharsets.UTF_8)));

        return value;
    }
}
