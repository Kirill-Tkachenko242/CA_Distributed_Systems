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
    private String sessionToken;
    
    // opens a gRPC connection to the server and creates a blocking
    public IdentityVerificationClient(String host, int port) {
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        this.blockingStub = IdentityVerificationServiceGrpc.newBlockingStub(channel);
    }
    
    // returns the session token from the last successful verification
    public String getSessionToken() {
        return sessionToken;
    }
    
    // closes the gRPC connection
    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }
    
    public String verifyIdentity(String studentId, String examId, String faceImageHash) {
        VerifyRequest request = VerifyRequest.newBuilder()
                .setStudentId(studentId)
                .setExamId(examId)
                .setFaceImageHash(faceImageHash)
                .build();
        
        io.grpc.Metadata metadata = new io.grpc.Metadata();
        io.grpc.Metadata.Key<String> clientIdKey =
                io.grpc.Metadata.Key.of("client-id", io.grpc.Metadata.ASCII_STRING_MARSHALLER);
        metadata.put(clientIdKey, "exam-gui-client");
        
        io.grpc.ClientInterceptor metadataInterceptor = new io.grpc.ClientInterceptor() {
            @Override
            public <ReqT, RespT> io.grpc.ClientCall<ReqT, RespT> interceptCall(
                    io.grpc.MethodDescriptor<ReqT, RespT> method,
                    io.grpc.CallOptions callOptions,
                    io.grpc.Channel next) {
                return new io.grpc.ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
                        next.newCall(method, callOptions)) {
                    @Override
                    public void start(Listener<RespT> responseListener, io.grpc.Metadata headers) {
                        headers.merge(metadata);
                        super.start(responseListener, headers);
                    }
                };
            }
        };
        var stubWithMetadata = blockingStub.withInterceptors(metadataInterceptor);
        
        VerifyResponse response;
        try {
            response = stubWithMetadata
                    .withDeadlineAfter(3, TimeUnit.SECONDS)
                    .verifyIdentity(request);
        } catch (io.grpc.StatusRuntimeException e) {
            if (e.getStatus().getCode() == io.grpc.Status.Code.DEADLINE_EXCEEDED) {
                return "RPC failed: server took too long to respond (deadline exceeded)";
            } else if (e.getStatus().getCode() == io.grpc.Status.Code.INVALID_ARGUMENT) {
                return "RPC failed: invalid input - " + e.getStatus().getDescription();
            }
            return "RPC failed: " + e.getMessage();
        }
        sessionToken = response.getSessionToken();
        
        return "Verified: " + response.getVerified() 
                + "\nSession Token: " + response.getSessionToken() 
                + "\nMessage: " + response.getMessage();
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
