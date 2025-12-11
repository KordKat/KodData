package hello1.koddata.engine.function;

import hello1.koddata.Main;
import hello1.koddata.engine.Value;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import hello1.koddata.net.UserClient;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

//Inheritance
public class DownloadFunction extends KodFunction<Object> {

//    Polymorphism
    @Override
    public Value<Object> execute() throws KException {
        if (!arguments.containsKey("fileName")) {
            throw new KException(ExceptionCode.KDE0012, "Function download need argument fileName");
        }
        Value<?> fileName = arguments.get("fileName");
        if (!(fileName.get() instanceof String fileNameS)) {
            throw new KException(ExceptionCode.KD00005, "only file can download");
        }

        if (!arguments.containsKey("UserClient")) {
            throw new KException(ExceptionCode.KDE0012, "Function download need argument UserClient");
        }
        Value<?> userClient = arguments.get("UserClient");
        if (!(userClient.get() instanceof UserClient uc)) {
            throw new KException(ExceptionCode.KD00005, "Invalid UserClient instance");
        }

        Path path = Main.bootstrap.getRootPath()
                .resolve("home")
                .resolve(uc.getUser().getUserData().name())
                .resolve(fileNameS);

        if (!Files.exists(path)) {
            throw new KException(ExceptionCode.KD00005, "File not found: " + fileNameS);
        }

        try {
            byte[] nameBytes = fileNameS.getBytes(StandardCharsets.UTF_8);
            byte[] fileData = Files.readAllBytes(path);
            int totalSize = 2 + 4 + nameBytes.length + 4 + fileData.length;

            ByteBuffer buffer = ByteBuffer.allocate(totalSize);

            buffer.put((byte) 'K');
            buffer.put((byte) 'D');
            buffer.putInt(nameBytes.length);
            buffer.put(nameBytes);
            buffer.putInt(fileData.length);
            buffer.put(fileData);

            buffer.flip();
            uc.write(buffer);

        } catch (IOException e) {
            e.printStackTrace();
            throw new KException(ExceptionCode.KD00005, "Error reading/sending file: " + e.getMessage());
        }

        return null;
    }
}