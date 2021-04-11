package jarg.thesis.experiments.benchmarks.javanio.server;

import jarg.thesis.experiments.benchmarks.utils.FileExporter;
import jarg.thesis.experiments.benchmarks.utils.Latency;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class NioServer {
    private static final Logger logger = LoggerFactory.getLogger(NioServer.class.getSimpleName());
    private String serverIp;
    private int port;
    private int messageSize;
    private int iterations;
    private int currentIteration;
    private Map<SocketChannel, ByteBuffer> socketConnections;
    private boolean exit;
    private FileExporter fileExporter;
    private Latency[] latencies;    // currently recorded latencies

    public NioServer(String serverIp, int port, int messageSize, int iterations){
        this.serverIp = serverIp;
        this.port = port;
        this.messageSize = messageSize;
        this.iterations = iterations;
        this.currentIteration = 0;
        this.socketConnections = new HashMap<>();
        this.exit = false;
        this.latencies = new Latency[iterations];
        for(int i=0; i<latencies.length; i++){
            latencies[i] = new Latency();
        }
        this.fileExporter = new FileExporter("tcp_nio_latencies_server.dat");
        StringBuilder fileHeadings = new StringBuilder();
        fileHeadings.append("# Server processing latencies ===================\r\n");
        fileHeadings.append("# Iterations : "+ iterations + "\r\n");
        fileHeadings.append("# Message size : "+ messageSize + "\r\n");
        fileHeadings.append("# Results ===================\r\n");
        fileExporter.exportText(fileHeadings.toString());
    }


    public void accept(SelectionKey key) throws IOException {
        ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
        SocketChannel clientChannel = serverChannel.accept();
        clientChannel.configureBlocking(false);
        clientChannel.register(key.selector(), SelectionKey.OP_READ);
        socketConnections.put(clientChannel, ByteBuffer.allocate(messageSize));
        logger.info("Connection accepted from : " + clientChannel.getRemoteAddress());
    }

    public void read(SelectionKey key) throws IOException {
        SocketChannel clientChannel = (SocketChannel) key.channel();
        ByteBuffer recvBuffer = socketConnections.get(clientChannel);

        // read data from the socket channel - record the time it takes
        Latency latency = latencies[currentIteration];
        latency.recordTimeStart();
        int bytesRead = 0;
        while(bytesRead < messageSize) {
            bytesRead += clientChannel.read(recvBuffer);
        }
        recvBuffer.flip();
        latency.recordTimeEnd();

        key.interestOps(SelectionKey.OP_WRITE);
    }

    public void write(SelectionKey key) throws IOException {
        SocketChannel clientSock = (SocketChannel) key.channel();
        ByteBuffer sendBuffer = socketConnections.get(clientSock);
        // echo the message to the client
        while(sendBuffer.hasRemaining()){
            clientSock.write(sendBuffer);
        }
        sendBuffer.clear();
        key.interestOps(SelectionKey.OP_READ);
        currentIteration++;
        if(currentIteration == iterations){
            exit = true;
        }
    }

    public void operate(){
        try {
            ServerSocketChannel serverChannel = ServerSocketChannel.open();
            serverChannel.bind(new InetSocketAddress(serverIp, port));
            serverChannel.configureBlocking(false);

            Selector selector = Selector.open();
            serverChannel.register(selector, SelectionKey.OP_ACCEPT);

            while(!exit){
                selector.select();
                // get the selected keys and handle any events
                Set<SelectionKey> selectionKeys = selector.selectedKeys();
                for(Iterator<SelectionKey> keysIter = selectionKeys.iterator(); keysIter.hasNext();){
                    SelectionKey key = keysIter.next();
                    keysIter.remove();
                    try{
                        if(key.isValid()){
                            if(key.isAcceptable()){
                                accept(key);
                            }else if(key.isReadable()){
                                read(key);
                            }else if(key.isWritable()){
                                write(key);
                            }
                        }
                    } catch (CancelledKeyException | IOException e) {
                        logger.error("Error during processing selected key.", e);
                    }
                    // Remember to remove closed sockets
                    socketConnections.keySet().removeIf(socket -> !socket.isOpen());
                }
            }
            fileExporter.exportLatencies(latencies);
            logger.info("Server shutdown.");
            serverChannel.close();

        } catch (IOException e) {
            logger.error("Error during server operation.", e);
        }
    }
}
