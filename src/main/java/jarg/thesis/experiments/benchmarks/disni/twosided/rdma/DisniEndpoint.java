package jarg.thesis.experiments.benchmarks.disni.twosided.rdma;

import com.ibm.disni.RdmaActiveEndpoint;
import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.verbs.IbvWC;
import com.ibm.disni.verbs.RdmaCmId;
import jarg.thesis.experiments.benchmarks.disni.twosided.client.TwoSidedClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * This endpoint will be using one buffer for the data to be sent and one for the
 * received data, which will be constantly reused.
 */
public class DisniEndpoint extends BasicDisniEndpoint {
    private static final Logger logger = LoggerFactory.getLogger(DisniEndpoint.class.getSimpleName());
    // Client
    private TwoSidedClient twoSidedClient;

    public DisniEndpoint(RdmaActiveEndpointGroup<? extends RdmaActiveEndpoint> group, RdmaCmId idPriv,
                         boolean serverSide, int maxBufferSize, TwoSidedClient client) throws IOException {
        super(group, idPriv, serverSide);
        this.twoSidedClient = client;
    }

    @Override
    public void dispatchCqEvent(IbvWC wc) throws IOException {
        int status = wc.getStatus();
        int wcOpcode = wc.getOpcode();

        if(status != 0){    // an error occurred
            logger.error("Cot completion event error. Status : " + status);
        }else{
            if (wcOpcode == IbvWC.IbvWcOpcode.IBV_WC_RECV.getOpcode()) {
                twoSidedClient.notifyOnMessageReception();
            }
        }
    }


}
