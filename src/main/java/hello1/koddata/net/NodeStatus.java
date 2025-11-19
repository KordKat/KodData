package hello1.koddata.net;

import java.nio.channels.SocketChannel;

public class NodeStatus {

    private volatile boolean isAvailable;
    private volatile float cpuLoad;
    private volatile float memoryLoad;
    private volatile float diskUsage;
    private volatile float fullDisk;
    private SocketChannel channel;
    private volatile int dataTransferPort;
    private volatile int clientPort;
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
}
