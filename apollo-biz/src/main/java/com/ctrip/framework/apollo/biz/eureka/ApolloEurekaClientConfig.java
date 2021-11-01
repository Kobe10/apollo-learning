package com.ctrip.framework.apollo.biz.eureka;


import com.ctrip.framework.apollo.biz.config.BizConfig;
import org.springframework.cloud.netflix.eureka.EurekaClientConfigBean;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.List;

@Component
@Primary
public class ApolloEurekaClientConfig extends EurekaClientConfigBean {

    private final BizConfig bizConfig;

    public ApolloEurekaClientConfig(final BizConfig bizConfig) {
        this.bizConfig = bizConfig;
    }

    /**
     * 调用 BizConfig#eurekaServiceUrls() 方法，从 ServerConfig 的 "eureka.service.url" 配置项，获得 Eureka Server 地址。
     * 一般都会有一个default的默认地址   zone也可能是一个list，逗号隔开
     * Eureka Server 共享该配置，从而形成 Eureka Server 集群。
     * Assert only one zone: defaultZone, but multiple environments.
     */
    public List<String> getEurekaServerServiceUrls(String myZone) {
        List<String> urls = bizConfig.eurekaServiceUrls();
        return CollectionUtils.isEmpty(urls) ? super.getEurekaServerServiceUrls(myZone) : urls;
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }
}
