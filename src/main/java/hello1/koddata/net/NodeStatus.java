package hello1.koddata.net;

import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;
import hello1.koddata.utils.Serializable;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class NodeStatus implements Serializable {

    private volatile boolean isAvailable;
    private volatile float cpuLoad;
    private volatile float memoryLoad;
    private volatile float diskUsage;
    private volatile float fullDisk;
    private SocketChannel channel;
    private volatile int dataTransferPort;
    private volatile int clientPort;
    private long lastHeartbeatTime;
    public boolean isAvailable() {
        return isAvailable;
    }

    public float getCpuLoad() {
        return cpuLoad;
    }

    public float getMemoryLoad() {
        return memoryLoad;
    }

    public float getDiskUsage() {
        return diskUsage;
    }

    public float getFullDisk() {
        return fullDisk;
    }

    public SocketChannel getChannel() {
        return channel;
    }


    public void setAvailable(boolean available) {
        isAvailable = available;
    }

    public void setChannel(SocketChannel channel) {
        this.channel = channel;
    }

    public void setCpuLoad(float cpuLoad) {
        this.cpuLoad = cpuLoad;
    }

    public void setDiskUsage(float diskUsage) {
        this.diskUsage = diskUsage;
    }

    public void setFullDisk(float fullDisk) {
        this.fullDisk = fullDisk;
    }

    public void setMemoryLoad(float memoryLoad) {
        this.memoryLoad = memoryLoad;
    }

    public int getDataTransferPort() {
        return dataTransferPort;
    }

    public void setDataTransferPort(int dataTransferPort) {
        this.dataTransferPort = dataTransferPort;
    }

    public int getClientPort() {
        return clientPort;
    }

    public void setClientPort(int clientPort) {
        this.clientPort = clientPort;
    }


    @Override
    public byte[] serialize() throws KException {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(
                    1 + (4 * 4) + (2 * 4)
            );

            buffer.put((byte) (isAvailable ? 1 : 0));
            buffer.putFloat(cpuLoad);
            buffer.putFloat(memoryLoad);
            buffer.putFloat(diskUsage);
            buffer.putFloat(fullDisk);
            buffer.putInt(dataTransferPort);
            buffer.putInt(clientPort);

            return buffer.array();
        } catch (Exception e) {
            throw new KException(ExceptionCode.KD00000, "Failed to serialize NodeStatus");
        }
    }

    @Override
    public void deserialize(byte[] b) {
        ByteBuffer buffer = ByteBuffer.wrap(b);

        this.isAvailable = buffer.get() == 1;
        this.cpuLoad = buffer.getFloat();
        this.memoryLoad = buffer.getFloat();
        this.diskUsage = buffer.getFloat();
        this.fullDisk = buffer.getFloat();
        this.dataTransferPort = buffer.getInt();
        this.clientPort = buffer.getInt();
    }

    public void setLastHeartbeatTime(long lastHeartbeatTime) {
        this.lastHeartbeatTime = lastHeartbeatTime;
    }

    public long getLastHeartbeatTime() {
        return lastHeartbeatTime;
    }
}
