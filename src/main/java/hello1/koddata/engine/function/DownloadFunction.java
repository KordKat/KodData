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

public class DownloadFunction extends KodFunction<Object> {

    @Override
    public Value<Object> execute() throws KException {
        // 1. Validate Argument: fileName
        if (!arguments.containsKey("fileName")) {
            throw new KException(ExceptionCode.KDE0012, "Function download need argument fileName");
        }
        Value<?> fileName = arguments.get("fileName");
        if (!(fileName.get() instanceof String fileNameS)) {
            throw new KException(ExceptionCode.KD00005, "only file can download");
        }

        // 2. Validate Argument: UserClient
        if (!arguments.containsKey("UserClient")) {
            throw new KException(ExceptionCode.KDE0012, "Function download need argument UserClient");
        }
        Value<?> userClient = arguments.get("UserClient");
        if (!(userClient.get() instanceof UserClient uc)) {
            throw new KException(ExceptionCode.KD00005, "Invalid UserClient instance");
        }

        // 3. Resolve Path
        Path path = Main.bootstrap.getRootPath()
                .resolve("home")
                .resolve(uc.getUser().getUserData().name())
                .resolve(fileNameS);

        if (!Files.exists(path)) {
            throw new KException(ExceptionCode.KD00005, "File not found: " + fileNameS);
        }

        try {
            // 4. Prepare Data
            byte[] nameBytes = fileNameS.getBytes(StandardCharsets.UTF_8);
            byte[] fileData = Files.readAllBytes(path); // อ่านไฟล์ทั้งหมดลง byte[]

            // คำนวณขนาด Buffer รวม: Header(KD) + NameLen + Name + DataLen + FileData
            int totalSize = 2 + 4 + nameBytes.length + 4 + fileData.length;

            ByteBuffer buffer = ByteBuffer.allocate(totalSize);

            // 5. Build Packet (ตาม Protocol KD)
            buffer.put((byte) 'K');                 // 1. Header K
            buffer.put((byte) 'D');                 // 2. Header D
            buffer.putInt(nameBytes.length);        // 3. Name Length
            buffer.put(nameBytes);                  // 4. Name Bytes
            buffer.putInt(fileData.length);         // 5. Data Length
            buffer.put(fileData);                   // 6. File Data

            // เตรียม Buffer สำหรับการอ่าน (Flip เพื่อให้ uc.write อ่านข้อมูลจากต้น Buffer)
            buffer.flip();

            // 6. Send via uc.write
            uc.write(buffer);

        } catch (IOException e) {
            e.printStackTrace();
            throw new KException(ExceptionCode.KD00005, "Error reading/sending file: " + e.getMessage());
        }

        return null;
    }
}