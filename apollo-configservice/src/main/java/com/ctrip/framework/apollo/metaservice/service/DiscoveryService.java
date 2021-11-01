package com.ctrip.framework.apollo.metaservice.service;

import com.ctrip.framework.apollo.core.ServiceNameConsts;
import com.ctrip.framework.apollo.tracer.Tracer;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.EurekaClient;
import com.netflix.discovery.shared.Application;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;

/**
 * 发现服务  获取不同模块的集群
 * 考虑到高可用，Meta Service 必须集群。因为 Meta Service 自身扮演了目录服务的角色，所以此时不得不引入 Proxy Server
 */
@Service
public class DiscoveryService {

    private final EurekaClient eurekaClient;

    public DiscoveryService(final EurekaClient eurekaClient) {
        this.eurekaClient = eurekaClient;
    }

    public List<InstanceInfo> getConfigServiceInstances() {
        Application application = eurekaClient.getApplication(ServiceNameConsts.APOLLO_CONFIGSERVICE);
        if (application == null) {
            Tracer.logEvent("Apollo.EurekaDiscovery.NotFound", ServiceNameConsts.APOLLO_CONFIGSERVICE);
        }
        return application != null ? application.getInstances() : Collections.emptyList();
    }

    public List<InstanceInfo> getMetaServiceInstances() {
        Application application = eurekaClient.getApplication(ServiceNameConsts.APOLLO_METASERVICE);
        if (application == null) {
            Tracer.logEvent("Apollo.EurekaDiscovery.NotFound", ServiceNameConsts.APOLLO_METASERVICE);
        }
        return application != null ? application.getInstances() : Collections.emptyList();
    }

    public List<InstanceInfo> getAdminServiceInstances() {
        Application application = eurekaClient.getApplication(ServiceNameConsts.APOLLO_ADMINSERVICE);
        if (application == null) {
            Tracer.logEvent("Apollo.EurekaDiscovery.NotFound", ServiceNameConsts.APOLLO_ADMINSERVICE);
        }
        return application != null ? application.getInstances() : Collections.emptyList();
    }
}
