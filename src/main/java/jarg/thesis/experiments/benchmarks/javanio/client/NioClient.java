package jarg.thesis.experiments.benchmarks.javanio.client;

import jarg.thesis.experiments.benchmarks.utils.FileExporter;
import jarg.thesis.experiments.benchmarks.utils.Latency;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class NioClient {
    private static final Logger logger = LoggerFactory.getLogger(NioClient.class.getSimpleName());
    private String serverIp;
    private int port;
    private int messageSize;
    private int iterations;
    private SocketChannel clientChannel;
    private ByteBuffer sendMessage;
    private ByteBuffer recvMessage;
    private Latency[] latencies;        // currently recorded latencies
    private FileExporter fileExporter;

    public NioClient(String serverIp, int port, int messageSize, int iterations) {
        this.serverIp = serverIp;
        this.port = port;
        this.messageSize = messageSize;
        this.iterations = iterations;
        this.sendMessage = ByteBuffer.allocate(messageSize);
        this.recvMessage = ByteBuffer.allocate(messageSize);
        this.latencies = new Latency[iterations];
        this.fileExporter = new FileExporter("tcp_nio_hrtt_c.dat");
    }

    public void operate(){
        InetSocketAddress severAddress = new InetSocketAddress(serverIp, port);
        try {
            clientChannel = SocketChannel.open(severAddress);
            // Prepare ------------------------------------
            StringBuilder fileHeadings = new StringBuilder();
            fileHeadings.append("# TCP NIO Benchmark ========\r\n");
            fileHeadings.append("# Iterations : "+ iterations + "\r\n");
            fileHeadings.append("# Message size : "+ messageSize + "\r\n");
            fileHeadings.append("# Results ===================\r\n");
            fileExporter.exportText(fileHeadings.toString());
            // message to send
            for(int i=0; i<messageSize; i++){
                byte data = 0b1;
                sendMessage.put(data);
            }
            sendMessage.flip();
            // pre-create latency objects. We will be reusing them during the benchmark.
            for(int i=0; i < latencies.length; i++){
                latencies[i] = new Latency();
            }
            // measure the full time of the benchmark as well
            Latency fullBenchmarkTime = new Latency();
            fullBenchmarkTime.recordTimeStart();
            // Start benchmark -----------------------------------------------------------------------------
            for(int currentIteration = 0; currentIteration < iterations; currentIteration++){
                Latency latency = latencies[currentIteration];
                // record start time ******************
                latency.recordTimeStart();

                // send message to server
                while(sendMessage.hasRemaining()){
                    clientChannel.write(sendMessage);
                }

                // read message from server
                int bytesRead = 0;
                while(bytesRead < messageSize) {
                    bytesRead += clientChannel.read(recvMessage);
                }
                // record end time ********************
                latencies[currentIteration].recordTimeEnd();
                // check for correctness
                sendMessage.rewind();
                recvMessage.rewind();
                for(int i=0; i<messageSize; i++){
                    if(sendMessage.get(i) != recvMessage.get(i)){
                        logger.error("Sent and echoed messages do not match!");
                        System.exit(1);
                    }
                    /* Correctness Check : put 0 on the receive buffer in order to make sure that in the next
                    iteration we will be reading the data received from the socket and not accidentally the data
                    received from the previous iteration. */
                    byte newData = 0b0;
                    recvMessage.put(newData);
                }
                sendMessage.rewind();
                recvMessage.clear();
            }
            // Finish benchmark --------------------------------------------------------------------------
            fullBenchmarkTime.recordTimeEnd();
            logger.info("Test took : " + ((float)fullBenchmarkTime.getLatency()/1000000000) + " seconds." );
            // Export recorded latencies
            fileExporter.exportLatencies(latencies);
            clientChannel.close();
        } catch (IOException e) {
            logger.error("Error during client operation.", e);
        }
    }
}
