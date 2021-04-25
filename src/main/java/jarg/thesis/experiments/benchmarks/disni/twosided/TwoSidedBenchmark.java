package jarg.thesis.experiments.benchmarks.disni.twosided;


import jarg.thesis.experiments.benchmarks.disni.twosided.client.TwoSidedClient;
import jarg.thesis.experiments.benchmarks.disni.twosided.server.TwoSidedServer;
import jarg.thesis.experiments.benchmarks.utils.RdmaBenchmarkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * This is a client/server benchmark that measures the latency of an RDMA library that
 * extends DiSNI (see IBM) and was developed for a MSc Thesis.
 * For this benchmark, the RDMA library uses <i>two-sided RDMA SEND</i>.
 * The client sends a message to the server and the server echoes this message back to
 * the client. Timestamps are taken in nanoseconds, in order to compute latencies.
 * This process runs up to a user-defined number of iterations.
 *</p>
 * <p>
 * The client code of the benchmark records timestamps before and after
 * sending a message and computes the difference between those timestamps.
 * At the end, the computed differences of each iteration are exported into a file.
 *</p>
 * <p>
 * On the other hand, the server records a timestamp before and after copying the
 * echoed message to a network buffer. It also computes the differences of these
 * timestamps and exports the differences computed in each iteration to a file.
 *</p>
 * <p>
 * Since client and server are located in different machines and export their data
 * into different files, a python script (check resources) is used to compute the final latency of
 * the benchmark. The script takes the two exported files as input and for each iteration,
 * it calculates the final latency estimate by subtracting the processing time on the server
 * from the recorded latency on the client.
 * Then each computed latency is divided by 2, in order to compute the RTT/2 latency.
 * </p>
 */
public class TwoSidedBenchmark {
    private static final Logger logger = LoggerFactory.getLogger(TwoSidedBenchmark.class.getSimpleName());
    private static final String RDMA_CONFIG = "rdma.bench.properties";

    public static void main(String[] args) {
        // Read cmd args
        if(args.length < 4){
           logger.info("Usage : <s for server, c for client> <server ip (not localhost)> <server port> <iterations>.");
           logger.info("E.g. for the server run with args : s 10.0.2.4 3000 100");
           logger.info("E.g. for a client of the previous server run with args : c 10.0.2.4 3000 100");
           System.exit(1);
        }

        String role = args[0];
        String host = args[1];
        String port = args[2];
        int iterations = Integer.parseInt(args[3]);

        // Read the passed RDMA config as well
        RdmaBenchmarkConfig benchmarkConfig = new RdmaBenchmarkConfig();
        try {
            benchmarkConfig.loadFromProperties(RDMA_CONFIG);
        } catch (Exception e) {
            logger.error("Could not read RDMA configuration. Exiting..");
            System.exit(1);
        }

        // run as a server
        if(role.equals("s")){
            TwoSidedServer server = new TwoSidedServer(host, port, benchmarkConfig, iterations);
            try {
                server.init();
                server.operate();
            } catch (Exception e) {
                logger.error("Error in operating server.", e);
                server.shutdown();
            }
        } else if(role.equals("c")) { // run as a client
            TwoSidedClient client = new TwoSidedClient(host, port, benchmarkConfig, iterations);
            try {
                client.init();
                client.operate();
            } catch (Exception e) {
                logger.error("Error in operating client.", e);
                client.shutdown();
            }
        }
    }
}
