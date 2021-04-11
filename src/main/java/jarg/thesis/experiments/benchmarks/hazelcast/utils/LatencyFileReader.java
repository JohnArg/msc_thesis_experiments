package jarg.thesis.experiments.benchmarks.hazelcast.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class LatencyFileReader {
    
    // Latencies in milli-seconds
    private static List<Long> AppendRequestLatencies;
    private static List<Long> PreVoteRequestLatencies;
    private static List<Long> VoteRequestLatencies;
    private static List<Long> InstallSnapshotLatencies;
    private static List<Long> TriggerLeaderElectionLatencies;

    // temporary data bins <request id, <member id, <time1, time2>>>
    private static Map<Integer, Map<String, List<Long>>> AppendRequests;
    private static Map<Integer, Map<String, List<Long>>> PrevoteRequests;
    private static Map<Integer, Map<String, List<Long>>> VoteRequests;
    private static Map<Integer, Map<String, List<Long>>> InstallSnapshotRequests;
    private static Map<Integer, Map<String, List<Long>>> TriggerLeaderElectionRequests;

    public static void main(String[] args) {

        AppendRequestLatencies = new ArrayList<>();
        PreVoteRequestLatencies = new ArrayList<>();
        VoteRequestLatencies = new ArrayList<>();
        InstallSnapshotLatencies = new ArrayList<>();
        TriggerLeaderElectionLatencies = new ArrayList<>();

        AppendRequests = new LinkedHashMap<>();
        PrevoteRequests = new LinkedHashMap<>();
        VoteRequests = new LinkedHashMap<>();
        InstallSnapshotRequests = new LinkedHashMap<>();
        TriggerLeaderElectionRequests = new LinkedHashMap<>();

        // calculate latencies
        List<Map<Integer, Map<String, List<Long>>>> allRequests = new ArrayList<>();
        allRequests.add(AppendRequests);
        allRequests.add(PrevoteRequests);
        allRequests.add(VoteRequests);
        allRequests.add(InstallSnapshotRequests);
        allRequests.add(TriggerLeaderElectionRequests);

        List<List<Long>> allLatencies = new ArrayList<>();
        allLatencies.add(AppendRequestLatencies);
        allLatencies.add(PreVoteRequestLatencies);
        allLatencies.add(VoteRequestLatencies);
        allLatencies.add(InstallSnapshotLatencies);
        allLatencies.add(TriggerLeaderElectionLatencies);

        process(args[0]);

        calculateLatencies(allRequests, allLatencies);
        // export results
        exportResults();
    }

    private static void process(String filename){
        int timestampsNum;
        try(FileReader fileReader = new FileReader("target/classes/" + filename);
            BufferedReader bufferedReader = new BufferedReader(fileReader)){
            // read the number of time stamps
            String line = bufferedReader.readLine();
            timestampsNum = Integer.parseInt(line.split("\\:")[1].trim());
            System.out.println(timestampsNum);
            // skip next line
            line = bufferedReader.readLine();
            // start reading timestamps
            while (true){
                line = bufferedReader.readLine();
                if(line == null){ //EOF
                    break;
                }
                String[] logParts = line.split(" ");
                recordData(logParts);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void recordData(String[] logData){
        // temporary data bins <request id, <member id, <time1, time2>>>
        String requestName = logData[0];
        String memberId = logData[1];
        Integer rpcId = Integer.parseInt(logData[2]);
        String creatorType = logData[3];
        Long time = Long.parseLong(logData[4]);
        Map<String, List<Long>> requestTimes;
        List<Long> timestamps;

        switch (requestName){
            case "PreVoteRequest":
                requestTimes = PrevoteRequests.get(rpcId);
                if(requestTimes == null) {
                    requestTimes = new LinkedHashMap<>();
                }
                timestamps = requestTimes.get(memberId);
                if(timestamps == null) {
                    timestamps = new ArrayList<>();
                }
                timestamps.add(time);
                requestTimes.put(memberId, timestamps);
                PrevoteRequests.put(rpcId, requestTimes);
                break;
            case "PreVoteResponse":
                requestTimes = PrevoteRequests.get(rpcId);
                timestamps = requestTimes.get(memberId);
                timestamps.add(time);
                break;
            case "VoteRequest":
                requestTimes = VoteRequests.get(rpcId);
                if(requestTimes == null) {
                    requestTimes = new LinkedHashMap<>();
                }
                timestamps = requestTimes.get(memberId);
                if(timestamps == null) {
                    timestamps = new ArrayList<>();
                }
                timestamps.add(time);
                requestTimes.put(memberId, timestamps);
                VoteRequests.put(rpcId, requestTimes);
                break;
            case "VoteResponse":
                requestTimes = VoteRequests.get(rpcId);
                timestamps = requestTimes.get(memberId);
                timestamps.add(time);
                break;
            case "AppendRequest":
                requestTimes = AppendRequests.get(rpcId);
                if(requestTimes == null) {
                    requestTimes = new LinkedHashMap<>();
                }
                timestamps = requestTimes.get(memberId);
                if(timestamps == null) {
                    timestamps = new ArrayList<>();
                }
                timestamps.add(time);
                requestTimes.put(memberId, timestamps);
                AppendRequests.put(rpcId, requestTimes);
                break;
            case "AppendSuccessResponse":
                requestTimes = AppendRequests.get(rpcId);
                timestamps = requestTimes.get(memberId);
                timestamps.add(time);
                break;
            case "AppendFailureResponse":
                requestTimes = AppendRequests.get(rpcId);
                timestamps = requestTimes.get(memberId);
                timestamps.add(time);
                break;
            case "InstallSnapshot":
                if(creatorType.equals("SENDER")){
                    requestTimes = InstallSnapshotRequests.get(rpcId);
                    if(requestTimes == null) {
                        requestTimes = new LinkedHashMap<>();
                    }
                    timestamps = requestTimes.get(memberId);
                    if(timestamps == null) {
                        timestamps = new ArrayList<>();
                    }
                    timestamps.add(time);
                    requestTimes.put(memberId, timestamps);
                    InstallSnapshotRequests.put(rpcId, requestTimes);
                }else{
                    requestTimes = InstallSnapshotRequests.get(rpcId);
                    timestamps = requestTimes.get(memberId);
                    timestamps.add(time);
                }
                break;
            case "TriggerLeaderElection":
                if(creatorType.equals("SENDER")){
                    requestTimes = TriggerLeaderElectionRequests.get(rpcId);
                    if(requestTimes == null) {
                        requestTimes = new LinkedHashMap<>();
                    }
                    timestamps = requestTimes.get(memberId);
                    if(timestamps == null) {
                        timestamps = new ArrayList<>();
                    }
                    timestamps.add(time);
                    requestTimes.put(memberId, timestamps);
                    TriggerLeaderElectionRequests.put(rpcId, requestTimes);
                }else{
                    requestTimes = TriggerLeaderElectionRequests.get(rpcId);
                    timestamps = requestTimes.get(memberId);
                    timestamps.add(time);
                }
                break;
            default:
                System.err.println("Error in parsing log line");
                return;
        }
    }

    private static void calculateLatencies(List<Map<Integer, Map<String, List<Long>>>> allRequests,
                                           List<List<Long>> allLatencies){
        // temporary data bins <request id, <member id, <time1, time2>>>
        int index = 0;
        for(Map<Integer, Map<String, List<Long>>> requestData : allRequests ){
            List<Long> latencyDatapoints = allLatencies.get(index);
            for(Integer rpcId : requestData.keySet()){
                Map<String, List<Long>> timestamps = requestData.get(rpcId);
                // check correctness ==========
                if(timestamps.keySet().size() != 1){
                    System.err.println("Error : more than one member recorded for " +
                            "RPC id : " + rpcId);
                    return;
                }
                // just need one member id
                String memberId = null;
                for(String member : timestamps.keySet()){
                    memberId = member;
                    break;
                }
                List<Long> times = timestamps.get(memberId);
                if(times.size() != 2){
                    System.err.println("[RPC "+rpcId+"]Expected 2 recorded times. Got "+times.size());
                }
                // Calculate latency ================
                latencyDatapoints.add((Math.abs(times.get(0) - times.get(1))/1000));
            }
            index ++;
        }
    }

    private static void exportResults(){
        String appendFileName = "AppendLatencies.dat";
        String prevoteFileName = "PreVoteLatencies.dat";
        String voteFileName = "VoteLatencies.dat";
        String snapshotFileName = "InstallSnapshotLatencies.dat";
        String leaderElectionFileName = "TriggerLeaderElectionLatencies.dat";

        String filePathPrefix = "target/classes/datafiles/rdma/";

        try(FileWriter appendFileWriter = new FileWriter(filePathPrefix + appendFileName, true);
            FileWriter prevoteFileWriter = new FileWriter(filePathPrefix + prevoteFileName, true);
            FileWriter voteFileWriter = new FileWriter(filePathPrefix + voteFileName, true);
            FileWriter snapshotFileWriter = new FileWriter(filePathPrefix + snapshotFileName, true);
            FileWriter leaderFileWriter = new FileWriter(filePathPrefix + leaderElectionFileName, true);
        ) {
            for(int i=0; i<AppendRequestLatencies.size(); i++){
                appendFileWriter.write(AppendRequestLatencies.get(i)+"\r\n");
            }
            for(int i=0; i<PreVoteRequestLatencies.size(); i++){
                prevoteFileWriter.write(PreVoteRequestLatencies.get(i)+"\r\n");
            }
            for(int i=0; i<VoteRequestLatencies.size(); i++){
                voteFileWriter.write(VoteRequestLatencies.get(i)+"\r\n");
            }
            for(int i=0; i<InstallSnapshotLatencies.size(); i++){
                snapshotFileWriter.write(InstallSnapshotLatencies.get(i)+"\r\n");
            }
            for(int i=0; i<TriggerLeaderElectionLatencies.size(); i++){
                leaderFileWriter.write(TriggerLeaderElectionLatencies.get(i)+"\r\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
