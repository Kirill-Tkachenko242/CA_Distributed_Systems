/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package distsys.ca_distributed_systems;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.ConcurrentHashMap;
import javax.jmdns.JmDNS;
import javax.jmdns.ServiceEvent;
import javax.jmdns.ServiceInfo;
import javax.jmdns.ServiceListener;

/**
 *
 * @author Kirill
 */
public class ServiceDiscovery {
    
    private JmDNS jmdns;
    private ConcurrentHashMap<String, ServiceInfo> discoveredServices = new ConcurrentHashMap<>();
    
    public ServiceDiscovery() throws IOException {
        this.jmdns = JmDNS.create(InetAddress.getLocalHost());
        
        jmdns.addServiceListener(Serviceregistrar.SERVICE_TYPE, new ServiceListener() {
            @Override
            public void serviceAdded(ServiceEvent event) {
                jmdns.requestServiceInfo(event.getType(), event.getName(), 3000);
            }
            
            @Override
            public void serviceResolved(ServiceEvent event) {
                ServiceInfo info = event.getInfo();
                discoveredServices.put(info.getName(), info);
                System.out.println("[jmDNS] Discovered service \"" + info.getName()
                        + "\" at " + info.getHostAddresses()[0] + ":" + info.getPort());
            }
            
            @Override
            public void serviceRemoved(ServiceEvent event) {
                discoveredServices.remove(event.getName());
                System.out.println("[jmDNS] Service \"" + event.getName() + "\" went offline");
            }
        });
    }
    
    public ServiceInfo getService(String serviceName) {
        return discoveredServices.get(serviceName);
    }
    
    public void close() throws IOException {
        jmdns.close();
    }
}
