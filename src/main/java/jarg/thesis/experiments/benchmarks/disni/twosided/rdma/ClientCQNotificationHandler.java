package jarg.thesis.experiments.benchmarks.disni.twosided.rdma;

import com.ibm.disni.verbs.IbvWC;
import jarg.jrcm.networking.dependencies.netrequests.WorkCompletionHandler;
import jarg.thesis.experiments.benchmarks.disni.twosided.client.TwoSidedClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientCQNotificationHandler implements WorkCompletionHandler {
    private Logger logger = LoggerFactory.getLogger(ClientCQNotificationHandler.class.getSimpleName());
    TwoSidedClient twoSidedClient;

    public ClientCQNotificationHandler(TwoSidedClient twoSidedClient){
        this.twoSidedClient = twoSidedClient;
    }

    @Override
    public void handleCqEvent(IbvWC wc) {
        int wcOpcode = wc.getOpcode();

        if (wcOpcode == IbvWC.IbvWcOpcode.IBV_WC_RECV.getOpcode()) {
            twoSidedClient.notifyOnMessageReception((int) wc.getWr_id());
        }
    }

    @Override
    public void handleCqEventError(IbvWC wc) {
        int status = wc.getStatus();
        logger.warn("Got completion event error :  {" +
                "Status : " + status + ", \n" +
                "Opcode : " + wc.getOpcode() + ", \n" +
                "WR id : " +wc.getWr_id() + "}");
    }
}
