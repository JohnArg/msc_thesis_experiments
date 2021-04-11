package jarg.thesis.experiments.benchmarks.disni.twosided.server;


import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.RdmaEndpointFactory;
import com.ibm.disni.verbs.RdmaCmId;
import jarg.thesis.experiments.benchmarks.disni.twosided.rdma.DisniServerEndpoint;

import java.io.IOException;

/**
 * Factory of server side RDMA endpoints.
 */
public class ServerEndpointFactory implements RdmaEndpointFactory<DisniServerEndpoint> {

    private RdmaActiveEndpointGroup<DisniServerEndpoint> endpointGroup;
    private int maxBufferSize;
    private int iterations;
    private TwoSidedServer server;

    public ServerEndpointFactory(RdmaActiveEndpointGroup<DisniServerEndpoint> endpointGroup,
                                 int maxBufferSize, int iterations, TwoSidedServer server) {
        this.endpointGroup = endpointGroup;
        this.maxBufferSize = maxBufferSize;
        this.iterations = iterations;
        this.server = server;
    }


    @Override
    public DisniServerEndpoint createEndpoint(RdmaCmId id, boolean serverSide) throws IOException {
        return new DisniServerEndpoint(endpointGroup, id, serverSide, maxBufferSize, server);
    }
}
