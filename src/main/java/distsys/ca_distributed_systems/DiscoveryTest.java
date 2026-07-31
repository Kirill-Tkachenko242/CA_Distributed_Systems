/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package distsys.ca_distributed_systems;

import javax.jmdns.ServiceInfo;

/**
 *
 * @author Kirill
 */
public class DiscoveryTest {
   public static void main(String[] args) throws Exception {
        ServiceDiscovery discovery = new ServiceDiscovery();
        System.out.println("Listening for services... (waiting 5 seconds for discovery)");
        Thread.sleep(5000);
        String[] namesToCheck = {"IdentityVerification", "ProctoringMonitor", "ExamSubmission"};
        for (String name : namesToCheck) {
            ServiceInfo info = discovery.getService(name);
            if (info != null) {
                System.out.println("FOUND: " + name + " at "
                        + info.getHostAddresses()[0] + ":" + info.getPort());
            } else {
                System.out.println("NOT FOUND: " + name + " (is its server running?)");
            }
        }
        discovery.close();
    } 
}
