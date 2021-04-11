package jarg.thesis.experiments.benchmarks.jrcm.twosided.client;

import com.ibm.disni.verbs.IbvWC;
import jarg.jrcm.networking.communicators.impl.ActiveRdmaCommunicator;
import jarg.jrcm.networking.dependencies.netrequests.AbstractWorkCompletionHandler;
import jarg.jrcm.networking.dependencies.netrequests.WorkRequestProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static jarg.jrcm.networking.dependencies.netrequests.types.WorkRequestType.TWO_SIDED_RECV;
import static jarg.jrcm.networking.dependencies.netrequests.types.WorkRequestType.TWO_SIDED_SEND_SIGNALED;

/**
 * Handles the completion of networking requests for the client RDMA endpoints.
 */
public class ClientCompletionHandler extends AbstractWorkCompletionHandler {
    private static final Logger logger = LoggerFactory.getLogger(ClientCompletionHandler.class.getSimpleName());
    private TwoSidedClient client;

    public ClientCompletionHandler(TwoSidedClient client){
        this.client = client;
    }

    @Override
    public void handleCqEvent(IbvWC workCompletionEvent) {
        // associate event with a Work Request
        WorkRequestProxy workRequestProxy = getProxyProvider().getWorkRequestProxyForWc(workCompletionEvent);
        // if this is a completion for a SEND
        if(workRequestProxy.getWorkRequestType().equals(TWO_SIDED_SEND_SIGNALED)){
            workRequestProxy.releaseWorkRequest();
        // else if this is a completion for a RECV
        }else if(workRequestProxy.getWorkRequestType().equals(TWO_SIDED_RECV)){
            // notify client
            client.notifyOnMessageReception(workRequestProxy);
        }
    }

    @Override
    public void handleCqEventError(IbvWC workCompletionEvent) {
        // associate event with a Work Request
        WorkRequestProxy workRequestProxy = getProxyProvider().getWorkRequestProxyForWc(workCompletionEvent);
        // Must free the request
        workRequestProxy.releaseWorkRequest();
        // Status 5 can happen on remote side disconnect, since we have already posted
        // RECV requests for that remote side. We can simply close the remote endpoint
        // at this point.
        if(workCompletionEvent.getStatus() == IbvWC.IbvWcStatus.IBV_WC_WR_FLUSH_ERR.ordinal()){
            ActiveRdmaCommunicator communicator = (ActiveRdmaCommunicator) workRequestProxy.getRdmaCommunicator();
            try {
                communicator.close();
            } catch (IOException | InterruptedException e) {
                logger.error("Error in closing endpoint.", e);
            }
        }else{
            logger.error("Error in network request "+ workCompletionEvent.getWr_id()
                    + " of status : " + workCompletionEvent.getStatus());
        }
    }
}
