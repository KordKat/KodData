package hello1.koddata.engine.function;

import hello1.koddata.Main;
import hello1.koddata.dataframe.CSVDataTransformer;
import hello1.koddata.dataframe.DataFrameRecord;
import hello1.koddata.dataframe.JsonDataTransformer;
import hello1.koddata.dataframe.loader.DataFrameLoader;
import hello1.koddata.engine.DataSource;
import hello1.koddata.engine.Value;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import hello1.koddata.sessions.users.User;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CompletableFuture;

public class ExportFunction extends KodFunction<CompletableFuture<DataFrameLoader>>{

    @Override
    public Value<CompletableFuture<DataFrameLoader>> execute() throws KException {
        if (!arguments.containsKey("dataName")){
            throw new KException(ExceptionCode.KDE0012,"Remove function need argument databaseName");
        }
        Value<?> dataName = arguments.get("dataName");
        if (!(dataName.get() instanceof DataFrameRecord[] dataNameDFR)) {
            throw new KException(ExceptionCode.KDE0012, "databaseName should be dataName");
        }
        if (!arguments.containsKey("dataType")){
            throw new KException(ExceptionCode.KDE0012,"Remove function need argument databaseName");
        }
        Value<?> dataType = arguments.get("dataType");
        if (!(dataType.get() instanceof DataSource dataTypeDS)) {
            throw new KException(ExceptionCode.KDE0012, "argument dataType should be DATABASE , JSON , CSV");
        }
        if (!arguments.containsKey("fileName")){
            throw new KException(ExceptionCode.KDE0012,"Remove function need argument databaseName");
        }
        Value<?> fileName = arguments.get("fileName");
        if (!(fileName.get() instanceof String fileNameString)) {
            throw new KException(ExceptionCode.KDE0012, "argument file name should be string");
        }
        if (dataTypeDS.equals(DataSource.CSV)){
            Value<?> userId = arguments.get("userId");
            if (!(userId.get() instanceof Long userIdLong)) {
                throw new KException(ExceptionCode.KDE0012, "user id should be long");
            }
            User user = Main.bootstrap.getUserManager().findUser(userIdLong);

            Path rootfs = Main.bootstrap.getRootPath();
            Path filePathPa = rootfs.resolve(user.getUserData().name());
            CSVDataTransformer csvDataTransformer = new CSVDataTransformer();
            String data = csvDataTransformer.transform(dataNameDFR);
            byte[] dataByteArray = data.getBytes(StandardCharsets.UTF_8);
            ByteBuffer dataByteBuffer = ByteBuffer.wrap(dataByteArray);
            try {
                try(AsynchronousFileChannel afc = AsynchronousFileChannel.open(
                        filePathPa.resolve(fileNameString),
                        StandardOpenOption.CREATE,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.READ)){
                    afc.write(dataByteBuffer , 0);
                };
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }
        else {
            Value<?> userId = arguments.get("userId");
            if (!(userId.get() instanceof Long userIdLong)) {
                throw new KException(ExceptionCode.KDE0012, "user id should be long");
            }
            User user = Main.bootstrap.getUserManager().findUser(userIdLong);

            Path rootfs = Main.bootstrap.getRootPath();
            Path filePathPa = rootfs.resolve(user.getUserData().name());
            JsonDataTransformer jsonDataTransformer = new JsonDataTransformer();
            String data = jsonDataTransformer.transform(dataNameDFR);
            byte[] dataByteArray = data.getBytes(StandardCharsets.UTF_8);
            ByteBuffer dataByteBuffer = ByteBuffer.wrap(dataByteArray);
            try(AsynchronousFileChannel afc = AsynchronousFileChannel.open(
                    filePathPa.resolve(fileNameString),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.READ)) {
                afc.write(dataByteBuffer, 0);
            }catch (IOException ex){
                throw new KException(ExceptionCode.KD00010, "Cannot export file: " + fileNameString + " ex: " + ex.getMessage());
            }
        }
        return null;
    }
}
