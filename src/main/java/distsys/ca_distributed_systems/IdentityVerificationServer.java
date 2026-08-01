/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package distsys.ca_distributed_systems;

import com.examproctoring.identity.IdentityVerificationServiceGrpc;
import com.examproctoring.identity.VerifyRequest;
import com.examproctoring.identity.VerifyResponse;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;

/**
 *
 * @author Kirill
 */

public class IdentityVerificationServer {
    
    private int port;
    private Server server;
    private Serviceregistrar registrar;
    
    public IdentityVerificationServer(int port) {
        this.port = port;
        this.server = ServerBuilder.forPort(port)
                .addService(new IdentityVerificationImpl())
                .build();
    }
    
    public void start() throws IOException {
        server.start();
        System.out.println("IdentityVerificationServer started, listening on port " + port);
        
        // register this service with jmDNS
        registrar = new Serviceregistrar();
        registrar.register("IdentityVerification", port);
        
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down IdentityVerificationServer...");
            IdentityVerificationServer.this.stop();
        }));
    }
    
    public void stop() {
        if (server != null) {
            server.shutdown();
        }
        if (registrar != null) {
            registrar.unregisterAll();
            try {
                registrar.close();
            } catch (IOException e) {
                System.err.println("Error closing jmDNS: " + e.getMessage());
            }
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
            
            // remote error handling: validate input, return proper gRPC status
            if (request.getStudentId() == null || request.getStudentId().isEmpty()) {
                responseObserver.onError(
                    io.grpc.Status.INVALID_ARGUMENT
                        .withDescription("studentId must not be empty")
                        .asRuntimeException()
                );
                return;
            }
            
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
