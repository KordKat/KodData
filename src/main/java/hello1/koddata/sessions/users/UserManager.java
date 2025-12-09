package hello1.koddata.sessions.users;

import hello1.koddata.Main;
import hello1.koddata.engine.Bootstrap;
import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import hello1.koddata.sessions.Session;
import hello1.koddata.utils.ByteUtils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Stream;

public class UserManager {
    private Map<Long, User> users;
    private Map<Long, UserData> userDataMap;
    private Map<String, Long> userIdNameMap;
    private Bootstrap bootstrap;
    private File userFile;
    private Path userFilePath;

    private static Properties props;

    public UserManager(Bootstrap bootstrap){
        users = new ConcurrentHashMap<>();
        userDataMap = new ConcurrentHashMap<>();
        userIdNameMap = new ConcurrentHashMap<>();
        this.bootstrap = bootstrap;
        this.userFilePath = bootstrap.getRootPath().resolve("home");
    }

    public User logIn(long userId ,String password){
        if(userDataMap.containsKey(userId)){
            UserData userData = userDataMap.get(userId);
            if(userData.password().equals(password)){
                if(users.containsKey(userId)) return users.get(userId);
                User user;
                if(userData.isAdmin()){
                    user = new Admin(userData, this, bootstrap.getSessionManager());
                }else {
                    user = new User(userData);
                }

                users.put(userId, user);
                return user;
            }
        }
        return null;
    }

    public long getUserId(String username) {
        return userIdNameMap.getOrDefault(username, -1L);
    }

    public void createUser(UserData data) throws KException {
        if(userDataMap.containsKey(data.userId())){
            throw new KException(ExceptionCode.KD00000, "user id already taken");
        }

        this.userDataMap.put(data.userId(), data);

    }

    public User findUser(long userId){
        return users.get(userId);
    }

    public UserData findUserData(long userId){
        return userDataMap.get(userId);
    }

    public List<User> userList(){
        return users.values().stream().toList();
    }

    public void logoutUser(long userId){
        if(users.containsKey(userId)){
            User user = users.get(userId);
            for(Session session : user.listSessions()){
                session.terminate();
            }
        }
    }

    public void loadAllUserData() throws IOException, KException {
        if (!Files.exists(userFilePath)) Files.createDirectories(userFilePath);

        List<Path> userPaths;
        try (Stream<Path> stream = Files.list(userFilePath)) {
            userPaths = stream.filter(Files::isDirectory).toList();
        } catch (RuntimeException e) {
            throw new KException(ExceptionCode.KD00010, e.getMessage());
        }

        for (Path path : userPaths) {
            Path cfg = path.resolve("usr.cfg");
            if (!Files.exists(cfg)) continue;

            byte[] data = Files.readAllBytes(cfg);
            ByteBuffer buffer = ByteBuffer.wrap(data);

            int expectedFields = 8;
            byte[][] fields = new byte[expectedFields][];

            for (int i = 0; i < expectedFields; i++) {
                if (buffer.remaining() < Integer.BYTES) {
                    throw new KException(ExceptionCode.KD00010, "Unexpected EOF while reading length of field " + i);
                }
                int length = buffer.getInt();

                if (length < 0 || length > buffer.remaining()) {
                    throw new KException(ExceptionCode.KD00010, "Invalid length for field " + i + ": " + length);
                }

                byte[] fieldData = new byte[length];
                buffer.get(fieldData);
                fields[i] = fieldData;
            }

            // Parse fields
            byte[] userIdField = fields[0];
            if (userIdField.length != Long.BYTES) {
                throw new KException(ExceptionCode.KD00010, "UserId field length invalid: " + userIdField.length);
            }
            long userId = ByteBuffer.wrap(userIdField).getLong();

            String name = new String(fields[1], StandardCharsets.UTF_8);
            String password = new String(fields[2], StandardCharsets.UTF_8);

            byte[] isAdminField = fields[3];
            if (isAdminField.length != 1) {
                throw new KException(ExceptionCode.KD00010, "isAdmin field length invalid: " + isAdminField.length);
            }
            boolean isAdmin = isAdminField[0] != 0;

            int maxSession = ByteUtils.bytesToInt(fields[4]);
            int maxProcessPerSession = ByteUtils.bytesToInt (fields[5]);
            int maxMemoryPerProcess = ByteUtils.bytesToInt(fields[6]);
            int maxStorageUsage = ByteUtils.bytesToInt(fields[7]);

            UserPrivilege privilege = new UserPrivilege(maxSession, maxProcessPerSession, maxMemoryPerProcess, maxStorageUsage);
            UserData userData = new UserData(userId, name, privilege, password, isAdmin);
            this.userDataMap.put(userId, userData);
            UserData.idCounter.next();
            System.out.println("Loaded user: " + userId);
        }
    }


    private static byte[][] splitBytesNoTrailingDelimiter(byte[] data, byte delimiter) {
        List<byte[]> parts = new ArrayList<>();
        int start = 0;
        for (int i = 0; i < data.length; i++) {
            if (data[i] == delimiter) {
                parts.add(Arrays.copyOfRange(data, start, i));
                start = i + 1;
            }
        }
        // Add the last part after last delimiter (or whole array if no delimiter)
        if (start <= data.length) {
            parts.add(Arrays.copyOfRange(data, start, data.length));
        }
        return parts.toArray(new byte[0][]);
    }


    public void saveUserData() throws IOException {

        if (!Files.exists(userFilePath)) {

            Files.createDirectories(userFilePath);

        }


        for (UserData userData : userDataMap.values()) {

            Path userDir = userFilePath.resolve(userData.name());

            if (!Files.exists(userDir)) {

                Files.createDirectories(userDir);

            }

            Path cfg = userDir.resolve("usr.cfg");


            // Prepare all fields as byte arrays first

            ByteBuffer userIdBuffer = ByteBuffer.allocate(Long.BYTES);

            userIdBuffer.putLong(userData.userId());

            byte[] userIdBytes = userIdBuffer.array();


            byte[] nameBytes = userData.name().getBytes(StandardCharsets.UTF_8);

            byte[] passwordBytes = userData.password().getBytes(StandardCharsets.UTF_8);

            byte[] isAdminBytes = new byte[] { (byte) (userData.isAdmin() ? 1 : 0) };


            UserPrivilege priv = userData.userPrivilege();

            byte[] maxSessionBytes = ByteUtils.intToBytes(priv.maxSession());

            byte[] maxProcessBytes = ByteUtils.intToBytes(priv.maxProcessPerSession());

            byte[] maxMemoryBytes = ByteUtils.intToBytes(priv.maxMemoryPerProcess());

            byte[] maxStorageBytes = ByteUtils.intToBytes(priv.maxStorageUsage());


            byte[][] fields = new byte[][] {

                    userIdBytes,

                    nameBytes,

                    passwordBytes,

                    isAdminBytes,

                    maxSessionBytes,

                    maxProcessBytes,

                    maxMemoryBytes,

                    maxStorageBytes

            };


            // Calculate total size needed for ByteBuffer:

            // for each field: 4 bytes (int length) + length of data

            int totalSize = 0;

            for (byte[] field : fields) {

                totalSize += Integer.BYTES + field.length;

            }


            ByteBuffer buffer = ByteBuffer.allocate(totalSize);


            for (byte[] field : fields) {

                buffer.putInt(field.length);

                buffer.put(field);

            }


            // Flip buffer to prepare for writing

            buffer.flip();


            Files.write(cfg, buffer.array());

        }

    }


    public void changeUserPassword(long userId, String newPass){
        if(userDataMap.containsKey(userId)){
            userDataMap.computeIfPresent(userId, (k, userData) -> userData.withPassword(newPass));
        }
    }

    public void removeUser(long id) throws IOException {
        if(userDataMap.containsKey(id)) {
            if(users.containsKey(id)) {
                User u = users.get(id);
                u.logOut();
                for (Session session : u.listSessions()) {
                    session.terminate();
                }
            }
            Path userPath = Main.bootstrap.getRootPath().resolve("home").resolve(userDataMap.get(id).name());
            Files.walkFileTree(userPath, new SimpleFileVisitor<Path>(){
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }
            });
            userDataMap.remove(id);
            users.remove(id);
        }
    }

    public String getProperty(String key, String defaultValue){
        return props.getProperty(key, defaultValue);
    }

    public void setProperty(String key, String value){
        props.setProperty(key, value);
    }

    public void removeProperty(String key){
        props.remove(key);
    }

    public UserData getUserDataByName(String name){
        Optional<UserData> op =userDataMap.entrySet().stream().filter(x -> x.getValue().name().equals(name)).map(x -> x.getValue()).findAny();
        return op.orElse(null);
    }


    public Map<Long, UserData> getUserDataMap() {
        return userDataMap;
    }
}
