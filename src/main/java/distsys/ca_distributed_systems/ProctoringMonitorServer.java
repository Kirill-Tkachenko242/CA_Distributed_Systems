/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package distsys.ca_distributed_systems;

/**
 *
 * @author Kirill
 */

import com.examproctoring.monitor.ActivityFrame;
import com.examproctoring.monitor.ActivitySummary;
import com.examproctoring.monitor.Alert;
import com.examproctoring.monitor.AlertSubscription;
import com.examproctoring.monitor.ProctoringMonitorServiceGrpc;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;

public class ProctoringMonitorServer {
    private int port;
    private Server server;
    
    public ProctoringMonitorServer(int port) {
        this.port = port;
        this.server = ServerBuilder.forPort(port)
                .addService(new ProctoringMonitorImpl())
                .build();
    }
    
    public void start() throws IOException {
        server.start();
        System.out.println("ProctoringMonitorServer started, listening on port " + port);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down ProctoringMonitorServer...");
            ProctoringMonitorServer.this.stop();
        }));
    }
    
    public void stop() {
        if (server != null) {
            server.shutdown();
        }
    }
    
    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }
    
    public static void main(String[] args) 
            throws IOException, InterruptedException {
        int port = 50052;
        ProctoringMonitorServer server = new ProctoringMonitorServer(port);
        server.start();
        server.blockUntilShutdown();
    } //main
    
    static class ProctoringMonitorImpl extends 
            ProctoringMonitorServiceGrpc.ProctoringMonitorServiceImplBase {
        @Override
        public StreamObserver<ActivityFrame> 
                streamActivityFeed(StreamObserver<ActivitySummary> responseObserver) {
            return new StreamObserver<ActivityFrame>() {
                int frameCount = 0;
                int flagCount = 0;
                
                @Override
                public void onNext(ActivityFrame frame) {
                    frameCount++;
                    System.out.println("Received frame #" + frameCount
                            + " motionScore=" + frame.getMotionScore()
                            + " audioLevel=" + frame.getAudioLevel()
                            + " gazeDirection=" + frame.getGazeDirection());
                    if (frame.getMotionScore() > 0.7f || frame.getAudioLevel() > 0.7f) {
                        flagCount++;
                    }
                }
                
                @Override
                public void onError(Throwable t) {
                    System.err.println("StreamActivityFeed error: " + t.getMessage());
                }
                
                @Override
                public void onCompleted() {
                    ActivitySummary summary = ActivitySummary.newBuilder()
                            .setFlagCount(flagCount)
                            .setSummary("Processed " + frameCount + " frames, "
                                    + flagCount + " suspicious event(s) detected")
                            .build();
                    responseObserver.onNext(summary);
                    responseObserver.onCompleted();
                }
            };
        }
        
        @Override
        public void liveAlertFeed(AlertSubscription request, StreamObserver<Alert> responseObserver) {
            System.out.println("Client subscribed to alerts for session: " + request.getSessionToken());
            try {
                String[] alertTypes = {"MultipleFacesDetected", "LookingAway", "AudioSpike"};
                
                for (int i = 0; i < alertTypes.length; i++) {
                    Alert alert = Alert.newBuilder()
                            .setAlertType(alertTypes[i])
                            .setSeverity(i + 1)
                            .setTimestamp(System.currentTimeMillis())
                            .build();
                    responseObserver.onNext(alert);
                    System.out.println("Sent alert: " + alertTypes[i]);
                    Thread.sleep(1000); // simulate a delay between alerts
                }
                responseObserver.onCompleted();
            } catch (InterruptedException e) {
                responseObserver.onError(e);
            }
        }
    }
} //class
