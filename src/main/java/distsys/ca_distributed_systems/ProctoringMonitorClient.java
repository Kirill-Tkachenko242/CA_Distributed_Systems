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
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ProctoringMonitorClient {
    private ManagedChannel channel;
    private ProctoringMonitorServiceGrpc.ProctoringMonitorServiceStub asyncStub;
    
    public ProctoringMonitorClient(String host, int port) {
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        this.asyncStub = ProctoringMonitorServiceGrpc.newStub(channel);
    }
    
    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }
    
    public void sendActivityFeed() throws InterruptedException {
        CountDownLatch finishLatch = new CountDownLatch(1);
        
        StreamObserver<ActivitySummary> responseObserver = 
                new StreamObserver<ActivitySummary>() {
            @Override
            public void onNext(ActivitySummary summary) {
                System.out.println("Final summary -> flagCount: " + summary.getFlagCount()
                        + ", summary: " + summary.getSummary());
            }
            
            @Override
            public void onError(Throwable t) {
                System.err.println("StreamActivityFeed failed: " + t.getMessage());
                finishLatch.countDown();
            }
            
            @Override
            public void onCompleted() {
                System.out.println("StreamActivityFeed completed by server.");
                finishLatch.countDown();
            }
        };
        StreamObserver<ActivityFrame> requestObserver = 
                asyncStub.streamActivityFeed(responseObserver);
        try {
            float[] motionScores = {0.1f, 0.85f, 0.2f, 0.9f, 0.15f};
            for (float motion : motionScores) {
                ActivityFrame frame = ActivityFrame.newBuilder()
                        .setTimestamp(System.currentTimeMillis())
                        .setMotionScore(motion)
                        .setAudioLevel(0.3f)
                        .setGazeDirection("forward")
                        .build();
                requestObserver.onNext(frame);
                Thread.sleep(300);
            }
        } catch (Exception e) {
            requestObserver.onError(e);
            return;
        }
        requestObserver.onCompleted();
        finishLatch.await(5, TimeUnit.SECONDS);
    }
    
    public void listenForAlerts(String sessionToken) throws InterruptedException {
        CountDownLatch finishLatch = new CountDownLatch(1);
        
        AlertSubscription subscription = AlertSubscription.newBuilder()
                .setSessionToken(sessionToken)
                .build();
        asyncStub.liveAlertFeed(subscription, new StreamObserver<Alert>() {
            @Override
            public void onNext(Alert alert) {
                System.out.println("ALERT received -> type: " + alert.getAlertType()
                        + ", severity: " + alert.getSeverity()
                        + ", timestamp: " + alert.getTimestamp());
            }
            
            @Override
            public void onError(Throwable t) {
                System.err.println("LiveAlertFeed failed: " + t.getMessage());
                finishLatch.countDown();
            }
            
            @Override
            public void onCompleted() {
                System.out.println("LiveAlertFeed stream completed by server.");
                finishLatch.countDown();
            }
        });
        finishLatch.await(10, TimeUnit.SECONDS);
    }
    
    public static void main(String[] args) throws InterruptedException {
        ProctoringMonitorClient client = new ProctoringMonitorClient("localhost", 50052);
        
        try {
            System.out.println("Testing client streaming (StreamActivityFeed)");
            client.sendActivityFeed();
            System.out.println("\nTesting server streaming (LiveAlertFeed)");
            client.listenForAlerts("sess-89f59cc1");
        } finally {
            client.shutdown();
        }
    } //main
} // class
