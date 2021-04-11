package jarg.thesis.experiments.benchmarks.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;

public class FileExporter {
    private static final Logger logger = LoggerFactory.getLogger(FileExporter.class.getSimpleName());
    private final String filename;


    public FileExporter(String filename) {
        this.filename = filename;
    }

    /**
     * Exports only the file headings to the output file.
     */
    public void exportText(String text){
        try(FileWriter fileWriter = new FileWriter(filename)){
            fileWriter.write(text);
        } catch (IOException e) {
            logger.error("Error while saving latency data to file.", e);
        }
    }

    /**
     * Exports latency data points to the output file.
     */
    public void exportLatencies(Latency[] latencies){
        try(FileWriter fileWriter = new FileWriter(filename, true)){
            for(int i=0; i<latencies.length; i++){
                fileWriter.write(String.valueOf(latencies[i].getLatency()) + "\r\n");
            }
        } catch (IOException e) {
            logger.error("Error while saving latency data to file.", e);
        }
    }

}
