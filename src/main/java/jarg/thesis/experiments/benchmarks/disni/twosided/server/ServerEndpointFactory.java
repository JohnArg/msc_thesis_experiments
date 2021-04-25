package jarg.thesis.experiments.benchmarks.disni.twosided.server;


import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.RdmaEndpointFactory;
import com.ibm.disni.verbs.RdmaCmId;
import jarg.jrcm.networking.dependencies.netrequests.WorkCompletionHandler;
import jarg.thesis.experiments.benchmarks.disni.twosided.rdma.DisniEndpoint;
import jarg.thesis.experiments.benchmarks.disni.twosided.rdma.ServerCQNotificationHandler;

import java.io.IOException;

/**
 * Factory of server side RDMA endpoints.
 */
public class ServerEndpointFactory implements RdmaEndpointFactory<DisniEndpoint> {

    private RdmaActiveEndpointGroup<DisniEndpoint> endpointGroup;
    private int maxBufferSize;
    private TwoSidedServer server;

    public ServerEndpointFactory(RdmaActiveEndpointGroup<DisniEndpoint> endpointGroup,
                                 int maxBufferSize, TwoSidedServer server) {
        this.endpointGroup = endpointGroup;
        this.maxBufferSize = maxBufferSize;
        this.server = server;
    }


    @Override
    public DisniEndpoint createEndpoint(RdmaCmId id, boolean serverSide) throws IOException {
        ServerCQNotificationHandler notificationHandler = new ServerCQNotificationHandler();
        DisniEndpoint endpoint = new DisniEndpoint(endpointGroup, id, serverSide, maxBufferSize,
                notificationHandler);
        notificationHandler.setTwoSidedServer(server);
        notificationHandler.setCommunicationsEndpoint(endpoint);
        return endpoint;
    }
}
