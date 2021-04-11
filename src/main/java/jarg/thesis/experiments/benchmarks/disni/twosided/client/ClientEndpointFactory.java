package jarg.thesis.experiments.benchmarks.disni.twosided.client;

import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.RdmaEndpointFactory;
import com.ibm.disni.verbs.RdmaCmId;
import jarg.thesis.experiments.benchmarks.disni.twosided.rdma.DisniEndpoint;

import java.io.IOException;

/**
 * Factory of client side RDMA endpoints.
 */
public class ClientEndpointFactory implements RdmaEndpointFactory<DisniEndpoint> {

    private RdmaActiveEndpointGroup<DisniEndpoint> endpointGroup;
    private int maxBufferSize;
    private TwoSidedClient client;

    public ClientEndpointFactory(RdmaActiveEndpointGroup<DisniEndpoint> endpointGroup, int maxBufferSize,
                                 TwoSidedClient client) {
        this.endpointGroup = endpointGroup;
        this.maxBufferSize = maxBufferSize;
        this.client = client;
    }


    @Override
    public DisniEndpoint createEndpoint(RdmaCmId id, boolean serverSide) throws IOException {
        return new DisniEndpoint(endpointGroup, id, serverSide, maxBufferSize, client);
    }
}
