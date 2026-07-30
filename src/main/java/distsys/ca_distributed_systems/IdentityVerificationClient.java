/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package distsys.ca_distributed_systems;

import com.examproctoring.identity.IdentityVerificationServiceGrpc;
import com.examproctoring.identity.VerifyRequest;
import com.examproctoring.identity.VerifyResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author Kirill
 */

public class IdentityVerificationClient {
    
    private ManagedChannel channel;
    private IdentityVerificationServiceGrpc.IdentityVerificationServiceBlockingStub blockingStub;
    
    public IdentityVerificationClient(String host, int port) {
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        this.blockingStub = IdentityVerificationServiceGrpc.newBlockingStub(channel);
    }
    
    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }
    
    public void verifyIdentity(String studentId, String examId, String faceImageHash) {
        VerifyRequest request = VerifyRequest.newBuilder()
                .setStudentId(studentId)
                .setExamId(examId)
                .setFaceImageHash(faceImageHash)
                .build();
        VerifyResponse response;
        try {
            response = blockingStub.verifyIdentity(request);
        } catch (Exception e) {
            System.err.println("RPC failed: " + e.getMessage());
            return;
        }
        
        System.out.println("verified: " + response.getVerified());
        System.out.println("sessionToken: " + response.getSessionToken());
        System.out.println("message: " + response.getMessage());
    }
    
    public static void main(String[] args) throws InterruptedException {
        IdentityVerificationClient client = new IdentityVerificationClient("localhost", 50051);
        try {
            client.verifyIdentity("20034521", "CS4021", "a91f7cd2e0b3");
        } finally {
            client.shutdown();
        }
    } //main
} //class
