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
import java.util.function.Consumer;

/**
 *
 * @author Kirill
 */

public class ExamSubmissionClient {
    
    private ManagedChannel channel;
    private ExamSubmissionServiceGrpc.ExamSubmissionServiceStub asyncStub;
    private ExamSubmissionServiceGrpc.ExamSubmissionServiceBlockingStub blockingStub;
    private StreamObserver<ChatMessage> chatRequestObserver;
    
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
    
    public void startChatSession(Consumer<String> onMessage) {
        chatRequestObserver = asyncStub.proctorChatSession(new StreamObserver<ChatMessage>() {
            @Override
            public void onNext(ChatMessage message) {
                onMessage.accept("[" + message.getSender() + "] " + message.getText());
            }
            
            @Override
            public void onError(Throwable t) {
                onMessage.accept("Chat error: " + t.getMessage());
            }
            
            @Override
            public void onCompleted() {
                onMessage.accept("Chat session closed by server.");
            }
        });
    }
    
    public void sendChatMessage(String text) {
        if (chatRequestObserver == null) {
            throw new IllegalStateException("Chat session not started - call startChatSession first");
        }
        ChatMessage message = ChatMessage.newBuilder()
                .setSender("student")
                .setText(text)
                .setTimestamp(System.currentTimeMillis())
                .build();
        chatRequestObserver.onNext(message);
    }
    
    public void closeChatSession() {
        if (chatRequestObserver != null) {
            chatRequestObserver.onCompleted();
            chatRequestObserver = null;
        }
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
            client.startChatSession(msg -> System.out.println(msg));
            client.sendChatMessage("Can I get more time?");
            Thread.sleep(500);
            client.closeChatSession();
            System.out.println("\nTesting unary RPC (SubmitExam)");
            System.out.println(client.submitExam("sess-89f59cc1", new String[]{"A", "C", "B"}));
        } finally {
            client.shutdown();
        }
    } // main
} // class
