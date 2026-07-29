/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package distsys.ca_distributed_systems;

/**
 *
 * @author Kirill
 */

import com.examproctoring.identity.IdentityVerificationServiceGrpc;
import com.examproctoring.identity.VerifyRequest;
import com.examproctoring.identity.VerifyResponse;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;

public class IdentityVerificationServer {
    private int port;
    private Server server;
    
    public IdentityVerificationServer(int port) {
        this.port = port;
        this.server = ServerBuilder.forPort(port)
                .addService(new IdentityVerificationImpl())
                .build();
    }
    
    public void start() throws IOException {
        server.start();
        System.out.println("IdentityVerificationServer started, listening on port " + port);
 
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down IdentityVerificationServer...");
            IdentityVerificationServer.this.stop();
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
    
    public static void main(String[] args) throws 
            IOException, InterruptedException {
        int port = 50051;
        IdentityVerificationServer server = new IdentityVerificationServer(port);
        server.start();
        server.blockUntilShutdown();
    } // main
    
    static class IdentityVerificationImpl extends 
            IdentityVerificationServiceGrpc.IdentityVerificationServiceImplBase {
        @Override
        public void verifyIdentity(VerifyRequest request, 
                StreamObserver<VerifyResponse> responseObserver) {
            
            System.out.println("Received verification request for studentId: " + 
                    request.getStudentId() + ", examId: " + request.getExamId());
            
            boolean verified = request.getFaceImageHash() != null
                    && !request.getFaceImageHash().isEmpty();
            
            String sessionToken = verified ? "sess-" + 
                    Integer.toHexString(request.getStudentId().hashCode()) : "";
            
            String message = verified ? "Identity confirmed"
                    : "Verification failed: no face image hash provided";
            
            VerifyResponse response = VerifyResponse.newBuilder()
                    .setVerified(verified)
                    .setSessionToken(sessionToken)
                    .setMessage(message)
                    .build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    } // actual service logic
} //class
