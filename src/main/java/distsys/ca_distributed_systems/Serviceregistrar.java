/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package distsys.ca_distributed_systems;

import javax.jmdns.JmDNS;
import javax.jmdns.ServiceInfo;
import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Kirill
 */

public class Serviceregistrar {
    
    public static String SERVICE_TYPE = "_examproctoring._tcp.local.";
    private JmDNS jmdns;
    private ServiceInfo serviceInfo;
    
    public Serviceregistrar() throws IOException {
        this.jmdns = JmDNS.create(InetAddress.getLocalHost());
    }
    
    public void register(String serviceName, int port) throws IOException {
        Map<String, String> props = new HashMap<>();
        props.put("protocol", "grpc");
        this.serviceInfo = ServiceInfo.create(SERVICE_TYPE, serviceName, port, 0, 0, props);
        jmdns.registerService(serviceInfo);
        System.out.println("[jmDNS] Registered service \"" + serviceName + "\" on port " + port + " (type " + SERVICE_TYPE + ")");
    }
    
    public void unregisterAll() {
        jmdns.unregisterAllServices();
    }
    
    public void close() throws IOException {
        jmdns.close();
    }
}
