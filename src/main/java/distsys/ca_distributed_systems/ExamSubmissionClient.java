/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package distsys.ca_distributed_systems;

import com.examproctoring.submission.ChatMessage;
import com.examproctoring.submission.ExamSubmissionServiceGrpc;
import com.examproctoring.submission.SubmissionRequest;
import com.examproctoring.submission.SubmissionResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author Kirill
 */

public class ExamSubmissionClient {
    
    private ManagedChannel channel;
    private ExamSubmissionServiceGrpc.ExamSubmissionServiceStub asyncStub;
    private ExamSubmissionServiceGrpc.ExamSubmissionServiceBlockingStub blockingStub;
    
    public ExamSubmissionClient(String host, int port) {
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        this.asyncStub = ExamSubmissionServiceGrpc.newStub(channel);
        this.blockingStub = ExamSubmissionServiceGrpc.newBlockingStub(channel);
    }
    
    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }
    
    public void runChatSession() throws InterruptedException {
        CountDownLatch finishLatch = new CountDownLatch(1);
        StreamObserver<ChatMessage> requestObserver = asyncStub.proctorChatSession(
                new StreamObserver<ChatMessage>() {
                    
                    @Override
                    public void onNext(ChatMessage message) {
                        System.out.println("[" + message.getSender() + "] " + message.getText());
                    }
                    
                    @Override
                    public void onError(Throwable t) {
                        System.err.println("ProctorChatSession failed: " + t.getMessage());
                        finishLatch.countDown();
                    }
                    
                    @Override
                    public void onCompleted() {
                        System.out.println("Chat session completed.");
                        finishLatch.countDown();
                    }
                });
        try {
            String[] studentMessages = {"Can I get more time?", "My internet connection is unstable."};
            
            for (String text : studentMessages) {
                ChatMessage message = ChatMessage.newBuilder()
                        .setSender("student")
                        .setText(text)
                        .setTimestamp(System.currentTimeMillis())
                        .build();
                requestObserver.onNext(message);
                Thread.sleep(500);
            }
        } catch (Exception e) {
            requestObserver.onError(e);
            return;
        }
        requestObserver.onCompleted();
        finishLatch.await(5, TimeUnit.SECONDS);
    }
    
    public String submitExam(String sessionToken, String[] answers) {
        SubmissionRequest request = SubmissionRequest.newBuilder()
                .setSessionToken(sessionToken)
                .addAllAnswers(java.util.Arrays.asList(answers))
                .setSubmittedAt(System.currentTimeMillis())
                .build();
        SubmissionResponse response;
        try {
            response = blockingStub.submitExam(request);
        } catch (Exception e) {
            System.err.println("SubmitExam RPC failed: " + e.getMessage());
            return "SubmitExam RPC failed: " + e.getMessage();
        }
        return "Accepted: " + response.getAccepted() + "\nIntegrity Flags: " 
                + response.getIntegrityFlags() 
                + "\nConfirmation ID: " + response.getConfirmationId();
    }
    
    public static void main(String[] args) throws InterruptedException {
        ExamSubmissionClient client = new ExamSubmissionClient("localhost", 50053);
        try {
            System.out.println("Testing bidirectional streaming (ProctorChatSession)");
            client.runChatSession();
            System.out.println("\nTesting unary RPC (SubmitExam)");
            client.submitExam("sess-89f59cc1", new String[]{"A", "C", "B"});
        } finally {
            client.shutdown();
        }
    } //main
} // class
