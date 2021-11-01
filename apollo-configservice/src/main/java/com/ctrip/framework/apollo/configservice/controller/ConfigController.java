package com.ctrip.framework.apollo.configservice.controller;

import com.ctrip.framework.apollo.biz.entity.Release;
import com.ctrip.framework.apollo.common.entity.AppNamespace;
import com.ctrip.framework.apollo.configservice.service.AppNamespaceServiceWithCache;
import com.ctrip.framework.apollo.configservice.service.config.ConfigService;
import com.ctrip.framework.apollo.configservice.util.InstanceConfigAuditUtil;
import com.ctrip.framework.apollo.configservice.util.NamespaceUtil;
import com.ctrip.framework.apollo.core.ConfigConsts;
import com.ctrip.framework.apollo.core.dto.ApolloConfig;
import com.ctrip.framework.apollo.core.dto.ApolloNotificationMessages;
import com.ctrip.framework.apollo.tracer.Tracer;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author Jason Song(song_s@ctrip.com)
 * @desc 提供读取配置的功能
 */
@RestController
@RequestMapping("/configs")
public class ConfigController {
    /**
     * 处理前缀：字符分割
     */
    private static final Splitter X_FORWARDED_FOR_SPLITTER = Splitter.on(",").omitEmptyStrings()
            .trimResults();
    private final ConfigService configService;
    private final AppNamespaceServiceWithCache appNamespaceService;
    private final NamespaceUtil namespaceUtil;
    private final InstanceConfigAuditUtil instanceConfigAuditUtil;
    private final Gson gson;

    private static final Type configurationTypeReference = new TypeToken<Map<String, String>>() {
    }.getType();

    public ConfigController(
            final ConfigService configService,
            final AppNamespaceServiceWithCache appNamespaceService,
            final NamespaceUtil namespaceUtil,
            final InstanceConfigAuditUtil instanceConfigAuditUtil,
            final Gson gson) {
        this.configService = configService;
        this.appNamespaceService = appNamespaceService;
        this.namespaceUtil = namespaceUtil;
        this.instanceConfigAuditUtil = instanceConfigAuditUtil;
        this.gson = gson;
    }

    /**
     * 获取配置文件接口
     *
     * @param appId
     * @param clusterName          一般情况下传入   default 即可。 如果希望配置按集群划分，可以参考集群独立配置说明做相关配置，然后在这里填入对应的集群名
     * @param namespace            如果没有新建过Namespace的话，传入application即可。 如果创建了Namespace，并且需要使用该Namespace的配置，则传入对应的Namespace名字。需要注意的是对于properties类型的namespace，只需要传入namespace的名字即可，如application。对于其它类型的namespace，需要传入namespace的名字加上后缀名，如datasources.json
     * @param dataCenter
     * @param clientSideReleaseKey 将上一次返回对象中的releaseKey传入即可，用来给服务端比较版本，如果版本比下来没有变化，则服务端直接返回304以节省流量和运算
     * @param clientIp             这个参数是可选的，用来实现灰度发布。
     * @param messagesAsString
     * @param request
     * @param response
     * @return {
     * "appId": "100004458",
     * "cluster": "default",
     * "namespaceName": "application",
     * "configurations": {
     * "portal.elastic.document.type":"biz",
     * "portal.elastic.cluster.name":"hermes-es-fws"
     * },
     * "releaseKey": "20170430092936-dee2d58e74515ff3"
     * }
     * @throws IOException
     */
    @GetMapping(value = "/{appId}/{clusterName}/{namespace:.+}")
    public ApolloConfig queryConfig(@PathVariable String appId, @PathVariable String clusterName,
                                    @PathVariable String namespace,
                                    @RequestParam(value = "dataCenter", required = false) String dataCenter,
                                    @RequestParam(value = "releaseKey", defaultValue = "-1") String clientSideReleaseKey,
                                    @RequestParam(value = "ip", required = false) String clientIp,
                                    @RequestParam(value = "messages", required = false) String messagesAsString,
                                    HttpServletRequest request, HttpServletResponse response) throws IOException {
        String originalNamespace = namespace;
        // 若 Namespace 名以 .properties 结尾，移除该结尾，并设置到 ApolloConfigNotification 中。
        // 例如 application.properties => application
        //strip out .properties suffix
        namespace = namespaceUtil.filterNamespaceName(namespace);
        // 获得归一化的 Namespace 名字。因为，客户端 Namespace 会填写错大小写。
        //fix the character case issue, such as FX.apollo <-> fx.apollo
        namespace = namespaceUtil.normalizeNamespace(appId, namespace);
        // 若 clientIp 未提交，从 Request 中获取。
        if (Strings.isNullOrEmpty(clientIp)) {
            //获取客户端IP、从请求的head中获取Ip
            clientIp = tryToGetClientIp(request);
        }
        // 解析 messagesAsString 参数，创建 ApolloNotificationMessages 对象。
        ApolloNotificationMessages clientMessages = transformMessages(messagesAsString);
        // 获得 Namespace 对应的 Release 对象
        List<Release> releases = Lists.newLinkedList();

        String appClusterNameLoaded = clusterName;
        if (!ConfigConsts.NO_APPID_PLACEHOLDER.equalsIgnoreCase(appId)) {
            // 获得 Release 对象
            Release currentAppRelease = configService.loadConfig(appId, clientIp, appId, clusterName, namespace,
                    dataCenter, clientMessages);

            if (currentAppRelease != null) {
                // 添加到 Release 数组中。
                releases.add(currentAppRelease);
                // 获得 Release 对应的 Cluster 名字
                //we have cluster search process, so the cluster name might be overridden
                appClusterNameLoaded = currentAppRelease.getClusterName();
            }
        }
        // 若 Namespace 为关联类型，则获取关联的 Namespace 的 Release 对象
        //if namespace does not belong to this appId, should check if there is a public configuration
        if (!namespaceBelongsToAppId(appId, namespace)) {
            Release publicRelease = this.findPublicConfig(appId, clientIp, clusterName, namespace,
                    dataCenter, clientMessages);
            if (!Objects.isNull(publicRelease)) {
                releases.add(publicRelease);
            }
        }
        // 若获得不到 Release ，返回状态码为 404 的响应
        if (releases.isEmpty()) {
            response.sendError(HttpServletResponse.SC_NOT_FOUND,
                    String.format(
                            "Could not load configurations with appId: %s, clusterName: %s, namespace: %s",
                            appId, clusterName, originalNamespace));
            Tracer.logEvent("Apollo.Config.NotFound",
                    assembleKey(appId, clusterName, originalNamespace, dataCenter));
            return null;
        }
        // 记录 InstanceConfig
        auditReleases(appId, clusterName, dataCenter, clientIp, releases);
        // 计算 Config Service 的合并 ReleaseKey
        String mergedReleaseKey = releases.stream().map(Release::getReleaseKey)
                .collect(Collectors.joining(ConfigConsts.CLUSTER_NAMESPACE_SEPARATOR));
        // 对比 Client 的合并 Release Key 。若相等，说明没有改变，返回状态码为 302 的响应
        if (mergedReleaseKey.equals(clientSideReleaseKey)) {
            // Client side configuration is the same with server side, return 304
            response.setStatus(HttpServletResponse.SC_NOT_MODIFIED);
            Tracer.logEvent("Apollo.Config.NotModified",
                    assembleKey(appId, appClusterNameLoaded, originalNamespace, dataCenter));
            return null;
        }
        // 创建 ApolloConfig 对象
        ApolloConfig apolloConfig = new ApolloConfig(appId, appClusterNameLoaded, originalNamespace,
                mergedReleaseKey);
        apolloConfig.setConfigurations(mergeReleaseConfigurations(releases));

        Tracer.logEvent("Apollo.Config.Found", assembleKey(appId, appClusterNameLoaded,
                originalNamespace, dataCenter));
        return apolloConfig;
    }

    private boolean namespaceBelongsToAppId(String appId, String namespaceName) {
        //Every app has an 'application' namespace
        if (Objects.equals(ConfigConsts.NAMESPACE_APPLICATION, namespaceName)) {
            return true;
        }

        //if no appId is present, then no other namespace belongs to it
        if (ConfigConsts.NO_APPID_PLACEHOLDER.equalsIgnoreCase(appId)) {
            return false;
        }

        AppNamespace appNamespace = appNamespaceService.findByAppIdAndNamespace(appId, namespaceName);

        return appNamespace != null;
    }

    /**
     * @param clientAppId the application which uses public config
     * @param namespace   the namespace
     * @param dataCenter  the datacenter
     */
    private Release findPublicConfig(String clientAppId, String clientIp, String clusterName,
                                     String namespace, String dataCenter, ApolloNotificationMessages clientMessages) {
        AppNamespace appNamespace = appNamespaceService.findPublicNamespaceByName(namespace);

        //check whether the namespace's appId equals to current one
        if (Objects.isNull(appNamespace) || Objects.equals(clientAppId, appNamespace.getAppId())) {
            return null;
        }

        String publicConfigAppId = appNamespace.getAppId();

        return configService.loadConfig(clientAppId, clientIp, publicConfigAppId, clusterName, namespace, dataCenter,
                clientMessages);
    }

    /**
     * Merge configurations of releases.
     * Release in lower index override those in higher index
     */
    Map<String, String> mergeReleaseConfigurations(List<Release> releases) {
        Map<String, String> result = Maps.newHashMap();
        for (Release release : Lists.reverse(releases)) {
            result.putAll(gson.fromJson(release.getConfigurations(), configurationTypeReference));
        }
        return result;
    }

    private String assembleKey(String appId, String cluster, String namespace, String dataCenter) {
        List<String> keyParts = Lists.newArrayList(appId, cluster, namespace);
        if (!Strings.isNullOrEmpty(dataCenter)) {
            keyParts.add(dataCenter);
        }
        return keyParts.stream().collect(Collectors.joining(ConfigConsts.CLUSTER_NAMESPACE_SEPARATOR));
    }

    private void auditReleases(String appId, String cluster, String dataCenter, String clientIp,
                               List<Release> releases) {
        if (Strings.isNullOrEmpty(clientIp)) {
            //no need to audit instance config when there is no ip
            return;
        }
        for (Release release : releases) {
            instanceConfigAuditUtil.audit(appId, cluster, dataCenter, clientIp, release.getAppId(),
                    release.getClusterName(),
                    release.getNamespaceName(), release.getReleaseKey());
        }
    }

    private String tryToGetClientIp(HttpServletRequest request) {
        String forwardedFor = request.getHeader("X-FORWARDED-FOR");
        if (!Strings.isNullOrEmpty(forwardedFor)) {
            return X_FORWARDED_FOR_SPLITTER.splitToList(forwardedFor).get(0);
        }
        return request.getRemoteAddr();
    }

    ApolloNotificationMessages transformMessages(String messagesAsString) {
        ApolloNotificationMessages notificationMessages = null;
        if (!Strings.isNullOrEmpty(messagesAsString)) {
            try {
                notificationMessages = gson.fromJson(messagesAsString, ApolloNotificationMessages.class);
            } catch (Throwable ex) {
                Tracer.logError(ex);
            }
        }

        return notificationMessages;
    }
}
