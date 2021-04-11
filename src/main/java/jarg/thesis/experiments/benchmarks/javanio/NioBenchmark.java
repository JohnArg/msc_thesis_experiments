package jarg.thesis.experiments.benchmarks.javanio;

import jarg.thesis.experiments.benchmarks.javanio.client.NioClient;
import jarg.thesis.experiments.benchmarks.javanio.server.NioServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NioBenchmark {
    private static final Logger logger = LoggerFactory.getLogger(NioBenchmark.class.getSimpleName());

    public static void main(String[] args) {
        // server side
        if(args.length == 5){
            String type = args[0];
            String serverIp = args[1];
            int port = Integer.parseInt(args[2]);
            int messageSize = Integer.parseInt(args[3]);
            int iterations = Integer.parseInt(args[4]);
            if(type.equals("s")){
                NioServer server = new NioServer(serverIp, port, messageSize, iterations);
                server.operate();
            }else if(type.equals("c")){
                NioClient client = new NioClient(serverIp, port, messageSize, iterations);
                client.operate();
            }

        }else{
            logger.info("Usage : <s for server OR c for client> <server ip> <server port> <message size> <iterations>");
            System.exit(1);
        }
    }
}
