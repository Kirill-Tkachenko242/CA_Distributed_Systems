/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package distsys.ca_distributed_systems;

/**
 *
 * @author Kirill
 */

import com.examproctoring.submission.ChatMessage;
import com.examproctoring.submission.ExamSubmissionServiceGrpc;
import com.examproctoring.submission.SubmissionRequest;
import com.examproctoring.submission.SubmissionResponse;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.UUID;

public class ExamSubmissionServer {
    private int port;
    private Server server;
    public ExamSubmissionServer(int port) {
        this.port = port;
        this.server = ServerBuilder.forPort(port)
                .addService(new ExamSubmissionImpl())
                .build();
    }
    public void start() throws IOException {
        server.start();
        System.out.println("ExamSubmissionServer started, listening on port " + port);
 
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down ExamSubmissionServer...");
            ExamSubmissionServer.this.stop();
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
    public static void main(String[] args) throws IOException, InterruptedException {
        int port = 50053;
        ExamSubmissionServer server = new ExamSubmissionServer(port);
        server.start();
        server.blockUntilShutdown();
    } // main
    static class ExamSubmissionImpl extends ExamSubmissionServiceGrpc.ExamSubmissionServiceImplBase {
        @Override
        public StreamObserver<ChatMessage> proctorChatSession(StreamObserver<ChatMessage> responseObserver) {
            return new StreamObserver<ChatMessage>() {
                
                @Override
                public void onNext(ChatMessage message) {
                    System.out.println("[" + message.getSender() + "] " + message.getText());
                    if ("student".equalsIgnoreCase(message.getSender())) {
                        ChatMessage reply = ChatMessage.newBuilder()
                                .setSender("proctor")
                                .setText("Message received: \"" + message.getText() + "\"")
                                .setTimestamp(System.currentTimeMillis())
                                .build();
                        responseObserver.onNext(reply);
                    }
                }
                
                @Override
                public void onError(Throwable t) {
                    System.err.println("ProctorChatSession error: " + t.getMessage());
                }
                
                @Override
                public void onCompleted() {
                    System.out.println("Chat session ended.");
                    responseObserver.onCompleted();
                }
            };
        }
        
        @Override
        public void submitExam(SubmissionRequest request, StreamObserver<SubmissionResponse> responseObserver) {
            System.out.println("Received exam submission for session: " + request.getSessionToken()
                    + " with " + request.getAnswersCount() + " answer(s)");
            int integrityFlags = request.getAnswersCount() == 0 ? 1 : 0;
            SubmissionResponse response = SubmissionResponse.newBuilder()
                    .setAccepted(true)
                    .setIntegrityFlags(integrityFlags)
                    .setConfirmationId("conf-" + UUID.randomUUID().toString().substring(0, 8))
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
} // class
