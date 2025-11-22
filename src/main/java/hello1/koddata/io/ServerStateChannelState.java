package hello1.koddata.io;

import hello1.koddata.concurrent.cluster.ConsistentCriteria;
import hello1.koddata.concurrent.cluster.Replica;
import hello1.koddata.net.DataTransferServer;
import hello1.koddata.utils.ref.ReplicatedResourceClusterReference;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;

public class ServerStateChannelState extends ChannelState {

    private final DataTransferServer dts;
    private final SocketChannel channel;
    private final boolean isFeedback;

    public ServerStateChannelState(int chunkSize, DataTransferServer dts, SocketChannel channel, boolean isFeedback) {
        super(chunkSize);
        this.dts = dts;
        this.channel = channel;
        this.isFeedback = isFeedback;
    }

    @Override
    public void perform() {
        try {
            int count = payloadBuffer.getInt();
            ByteArrayOutputStream feedbackBos = new ByteArrayOutputStream();
            int updatesNeeded = 0;

            feedbackBos.write(ByteBuffer.allocate(4).putInt(0).array());

            for (int i = 0; i < count; i++) {
                int nameLen = payloadBuffer.getInt();
                byte[] nameBytes = new byte[nameLen];
                payloadBuffer.get(nameBytes);
                String name = new String(nameBytes, StandardCharsets.UTF_8);

                int critLen = payloadBuffer.getInt();
                byte[] critBytes = new byte[critLen];
                payloadBuffer.get(critBytes);

                ReplicatedResourceClusterReference<?> localRef = ReplicatedResourceClusterReference.resources.get(name);
                if (localRef != null) {
                    Replica localReplica = localRef.get();
                    Replica remoteReplica = localReplica.getClass().newInstance();
                    remoteReplica.getConsistencyCriteria().deserialize(critBytes);

                    if (isFeedback) {
                        ConsistentCriteria consistentCriteria = new ConsistentCriteria();
                        consistentCriteria.deserialize(critBytes);
                        if(!consistentCriteria.isNewerThan(localRef.get().getConsistencyCriteria())) {
                            localRef.get().getConsistencyCriteria().deserialize(critBytes);
                        }
                    } else {
                        if (remoteReplica.getConsistencyCriteria().isNewerThan(localReplica.getConsistencyCriteria())) {
                            localRef.get().getConsistencyCriteria().deserialize(critBytes);
                        } else if (localReplica.getConsistencyCriteria().isNewerThan(remoteReplica.getConsistencyCriteria())) {
                            feedbackBos.write(ByteBuffer.allocate(4).putInt(nameBytes.length).array());
                            feedbackBos.write(nameBytes);
                            byte[] localData = localReplica.getConsistencyCriteria().serialize();
                            feedbackBos.write(ByteBuffer.allocate(4).putInt(localData.length).array());
                            feedbackBos.write(localData);
                            updatesNeeded++;
                        }
                    }
                }
            }

            if (!isFeedback && updatesNeeded > 0) {
                byte[] data = feedbackBos.toByteArray();
                ByteBuffer temp = ByteBuffer.wrap(data);
                temp.putInt(updatesNeeded);
                dts.sendFeedback(channel, data);
            }

        } catch (Exception ignored) {
        }
    }
}