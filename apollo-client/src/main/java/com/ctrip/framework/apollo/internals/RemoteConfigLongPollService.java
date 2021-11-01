package com.ctrip.framework.apollo.internals;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.escape.Escaper;
import com.google.common.net.UrlEscapers;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.RateLimiter;
import com.google.gson.Gson;

import com.ctrip.framework.apollo.build.ApolloInjector;
import com.ctrip.framework.apollo.core.ConfigConsts;
import com.ctrip.framework.apollo.core.dto.ApolloConfigNotification;
import com.ctrip.framework.apollo.core.dto.ApolloNotificationMessages;
import com.ctrip.framework.apollo.core.dto.ServiceDTO;
import com.ctrip.framework.apollo.core.enums.ConfigFileFormat;
import com.ctrip.framework.apollo.core.schedule.ExponentialSchedulePolicy;
import com.ctrip.framework.apollo.core.schedule.SchedulePolicy;
import com.ctrip.framework.apollo.core.utils.ApolloThreadFactory;
import com.ctrip.framework.apollo.exceptions.ApolloConfigException;
import com.ctrip.framework.apollo.tracer.Tracer;
import com.ctrip.framework.apollo.tracer.spi.Transaction;
import com.ctrip.framework.apollo.util.ConfigUtil;
import com.ctrip.framework.apollo.util.ExceptionUtil;
import com.ctrip.framework.apollo.util.http.HttpRequest;
import com.ctrip.framework.apollo.util.http.HttpResponse;
import com.ctrip.framework.apollo.util.http.HttpUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
public class RemoteConfigLongPollService {
    private static final Logger logger = LoggerFactory.getLogger(RemoteConfigLongPollService.class);
    private static final Joiner STRING_JOINER = Joiner.on(ConfigConsts.CLUSTER_NAMESPACE_SEPARATOR);
    private static final Joiner.MapJoiner MAP_JOINER = Joiner.on("&").withKeyValueSeparator("=");
    private static final Escaper queryParamEscaper = UrlEscapers.urlFormParameterEscaper();
    private static final long INIT_NOTIFICATION_ID = ConfigConsts.NOTIFICATION_ID_PLACEHOLDER;
    //90 seconds, should be longer than server side's long polling timeout, which is now 60 seconds
    private static final int LONG_POLLING_READ_TIMEOUT = 90 * 1000;
    /**
     * 长轮询 ExecutorService
     */
    private final ExecutorService m_longPollingService;
    /**
     * 是否停止长轮询的标识
     */
    private final AtomicBoolean m_longPollingStopped;
    /**
     * 失败定时重试策略，使用 {@link ExponentialSchedulePolicy}
     */
    private SchedulePolicy m_longPollFailSchedulePolicyInSecond;
    /**
     * 长轮询的 RateLimiter
     */
    private RateLimiter m_longPollRateLimiter;
    /**
     * 是否长轮询已经开始的标识
     */
    private final AtomicBoolean m_longPollStarted;
    /**
     * 长轮询的 Namespace Multimap 缓存
     * <p>
     * 通过 {@link #submit(String, RemoteConfigRepository)} 添加 RemoteConfigRepository 。
     * <p>
     * KEY：Namespace 的名字
     * VALUE：RemoteConfigRepository 集合
     */
    private final Multimap<String, RemoteConfigRepository> m_longPollNamespaces;
    /**
     * 通知编号 Map 缓存
     * <p>
     * KEY：Namespace 的名字
     * VALUE：最新的通知编号
     */
    private final ConcurrentMap<String, Long> m_notifications;
    /**
     * 通知消息 Map 缓存
     * <p>
     * KEY：Namespace 的名字
     * VALUE：ApolloNotificationMessages 对象
     */
    private final Map<String, ApolloNotificationMessages> m_remoteNotificationMessages;//namespaceName -> watchedKey -> notificationId
    private Type m_responseType;
    private Gson gson;
    private ConfigUtil m_configUtil;
    private HttpUtil m_httpUtil;
    private ConfigServiceLocator m_serviceLocator;

    /**
     * Constructor.
     */
    public RemoteConfigLongPollService() {
        // 单线程
        m_longPollFailSchedulePolicyInSecond = new ExponentialSchedulePolicy(1, 120); //in second
        m_longPollingStopped = new AtomicBoolean(false);
        // 单线程
        m_longPollingService = Executors.newSingleThreadExecutor(
                ApolloThreadFactory.create("RemoteConfigLongPollService", true));
        m_longPollStarted = new AtomicBoolean(false);
        // 初始化map 支持并发的map
        m_longPollNamespaces =
                Multimaps.synchronizedSetMultimap(HashMultimap.<String, RemoteConfigRepository>create());
        m_notifications = Maps.newConcurrentMap();
        m_remoteNotificationMessages = Maps.newConcurrentMap();
        m_responseType = new TypeToken<List<ApolloConfigNotification>>() {
        }.getType();
        gson = new Gson();
        m_configUtil = ApolloInjector.getInstance(ConfigUtil.class);
        m_httpUtil = ApolloInjector.getInstance(HttpUtil.class);
        m_serviceLocator = ApolloInjector.getInstance(ConfigServiceLocator.class);
        m_longPollRateLimiter = RateLimiter.create(m_configUtil.getLongPollQPS());
    }

    /**
     * 线程池定时任务 -- 长轮询任务提交
     *
     * @param namespace
     * @param remoteConfigRepository
     * @return
     */
    public boolean submit(String namespace, RemoteConfigRepository remoteConfigRepository) {
        boolean added = m_longPollNamespaces.put(namespace, remoteConfigRepository);
        m_notifications.putIfAbsent(namespace, INIT_NOTIFICATION_ID);
        if (!m_longPollStarted.get()) {
            startLongPolling();
        }
        return added;
    }

    private void startLongPolling() {
        // CAS 设置长轮询任务已经启动。若已经启动，不重复启动。
        if (!m_longPollStarted.compareAndSet(false, true)) {
            //already started
            return;
        }
        try {
            final String appId = m_configUtil.getAppId();
            final String cluster = m_configUtil.getCluster();
            final String dataCenter = m_configUtil.getDataCenter();
            final long longPollingInitialDelayInMills = m_configUtil.getLongPollingInitialDelayInMills();
            m_longPollingService.submit(new Runnable() {
                @Override
                public void run() {
                    if (longPollingInitialDelayInMills > 0) {
                        try {
                            logger.debug("Long polling will start in {} ms.", longPollingInitialDelayInMills);
                            TimeUnit.MILLISECONDS.sleep(longPollingInitialDelayInMills);
                        } catch (InterruptedException e) {
                            //ignore
                        }
                    }
                    // 长轮询任务--开始执行
                    doLongPollingRefresh(appId, cluster, dataCenter);
                }
            });
        } catch (Throwable ex) {
            m_longPollStarted.set(false);
            ApolloConfigException exception =
                    new ApolloConfigException("Schedule long polling refresh failed", ex);
            Tracer.logError(exception);
            logger.warn(ExceptionUtil.getDetailMessage(exception));
        }
    }

    void stopLongPollingRefresh() {
        this.m_longPollingStopped.compareAndSet(false, true);
    }

    /**
     * 长轮询
     *
     * @param appId
     * @param cluster
     * @param dataCenter
     */
    private void doLongPollingRefresh(String appId, String cluster, String dataCenter) {
        final Random random = new Random();
        ServiceDTO lastServiceDto = null;
        // 循环执行，直到停止或线程中断
        while (!m_longPollingStopped.get() && !Thread.currentThread().isInterrupted()) {
            //限流重试
            if (!m_longPollRateLimiter.tryAcquire(5, TimeUnit.SECONDS)) {
                //wait at most 5 seconds
                try {
                    TimeUnit.SECONDS.sleep(5);
                } catch (InterruptedException e) {
                }
            }
            Transaction transaction = Tracer.newTransaction("Apollo.ConfigService", "pollNotification");
            String url = null;
            try {
                // 获得 Config Service 的地址
                if (lastServiceDto == null) {
                    List<ServiceDTO> configServices = getConfigServices();
                    lastServiceDto = configServices.get(random.nextInt(configServices.size()));
                }
                //长轮询 Config Service 的配置变更通知 /notifications / v2 接口的 URL ，
                url = assembleLongPollRefreshUrl(lastServiceDto.getHomepageUrl(), appId, cluster, dataCenter,
                        m_notifications);

                logger.debug("Long polling from {}", url);
                HttpRequest request = new HttpRequest(url);
                request.setReadTimeout(LONG_POLLING_READ_TIMEOUT);

                transaction.addData("Url", url);

                final HttpResponse<List<ApolloConfigNotification>> response =
                        m_httpUtil.doGet(request, m_responseType);

                logger.debug("Long polling response: {}, url: {}", response.getStatusCode(), url);
                // 有新的通知，刷新本地的缓存---轮询得到新的通知、说明配置变化，此时可以通知调用获取配置接口
                if (response.getStatusCode() == 200 && response.getBody() != null) {
                    // 更新 m_notifications
                    updateNotifications(response.getBody());
                    // 更新 m_remoteNotificationMessages
                    updateRemoteNotifications(response.getBody());
                    transaction.addData("Result", response.getBody().toString());
                    // 通知对应的 RemoteConfigRepository 们---获取配置信息
                    notify(lastServiceDto, response.getBody());
                }

                //try to load balance
                // 无新的通知，重置连接的 Config Service 的地址，下次请求不同的 Config Service ，实现负载均衡。
                // 随机策略实现负载均衡
                if (response.getStatusCode() == 304 && random.nextBoolean()) {
                    lastServiceDto = null;
                }

                m_longPollFailSchedulePolicyInSecond.success();
                transaction.addData("StatusCode", response.getStatusCode());
                transaction.setStatus(Transaction.SUCCESS);
            } catch (Throwable ex) {
                lastServiceDto = null;
                Tracer.logEvent("ApolloConfigException", ExceptionUtil.getDetailMessage(ex));
                transaction.setStatus(ex);
                long sleepTimeInSecond = m_longPollFailSchedulePolicyInSecond.fail();
                logger.warn(
                        "Long polling failed, will retry in {} seconds. appId: {}, cluster: {}, namespaces: {}, long polling url: {}, reason: {}",
                        sleepTimeInSecond, appId, cluster, assembleNamespaces(), url, ExceptionUtil.getDetailMessage(ex));
                try {
                    TimeUnit.SECONDS.sleep(sleepTimeInSecond);
                } catch (InterruptedException ie) {
                    //ignore
                }
            } finally {
                transaction.complete();
            }
        }
    }

    /**
     * 更新 m_remoteNotificationMessages
     *
     * @param lastServiceDto
     * @param notifications
     */
    private void notify(ServiceDTO lastServiceDto, List<ApolloConfigNotification> notifications) {
        if (notifications == null || notifications.isEmpty()) {
            return;
        }
        // 循环 ApolloConfigNotification
        for (ApolloConfigNotification notification : notifications) {
            String namespaceName = notification.getNamespaceName();
            //create a new list to avoid ConcurrentModificationException
            // 创建 RemoteConfigRepository 数组，避免并发问题
            List<RemoteConfigRepository> toBeNotified =
                    Lists.newArrayList(m_longPollNamespaces.get(namespaceName));
            // 因为 .properties 在默认情况下被过滤掉，所以我们需要检查是否有监听器。若有，添加到 RemoteConfigRepository 数组
            ApolloNotificationMessages originalMessages = m_remoteNotificationMessages.get(namespaceName);
            // 获得远程的 ApolloNotificationMessages 对象，并克隆
            ApolloNotificationMessages remoteMessages = originalMessages == null ? null : originalMessages.clone();
            //since .properties are filtered out by default, so we need to check if there is any listener for it
            toBeNotified.addAll(m_longPollNamespaces
                    .get(String.format("%s.%s", namespaceName, ConfigFileFormat.Properties.getValue())));
            // 循环 RemoteConfigRepository ，进行通知
            for (RemoteConfigRepository remoteConfigRepository : toBeNotified) {
                try {
                    remoteConfigRepository.onLongPollNotified(lastServiceDto, remoteMessages);
                } catch (Throwable ex) {
                    Tracer.logError(ex);
                }
            }
        }
    }

    /**
     * 更新 m_notifications
     *
     * @param deltaNotifications
     */
    private void updateNotifications(List<ApolloConfigNotification> deltaNotifications) {
        // 循环 ApolloConfigNotification
        for (ApolloConfigNotification notification : deltaNotifications) {
            if (Strings.isNullOrEmpty(notification.getNamespaceName())) {
                continue;
            }
            // 更新 m_notifications
            String namespaceName = notification.getNamespaceName();
            if (m_notifications.containsKey(namespaceName)) {
                m_notifications.put(namespaceName, notification.getNotificationId());
            }
            // 因为 .properties 在默认情况下被过滤掉，所以我们需要检查是否有 .properties 后缀的通知。如有，更新 m_notifications
            //since .properties are filtered out by default, so we need to check if there is notification with .properties suffix
            String namespaceNameWithPropertiesSuffix =
                    String.format("%s.%s", namespaceName, ConfigFileFormat.Properties.getValue());
            if (m_notifications.containsKey(namespaceNameWithPropertiesSuffix)) {
                m_notifications.put(namespaceNameWithPropertiesSuffix, notification.getNotificationId());
            }
        }
    }

    private void updateRemoteNotifications(List<ApolloConfigNotification> deltaNotifications) {
        for (ApolloConfigNotification notification : deltaNotifications) {
            if (Strings.isNullOrEmpty(notification.getNamespaceName())) {
                continue;
            }

            if (notification.getMessages() == null || notification.getMessages().isEmpty()) {
                continue;
            }

            ApolloNotificationMessages localRemoteMessages =
                    m_remoteNotificationMessages.get(notification.getNamespaceName());
            if (localRemoteMessages == null) {
                localRemoteMessages = new ApolloNotificationMessages();
                m_remoteNotificationMessages.put(notification.getNamespaceName(), localRemoteMessages);
            }

            localRemoteMessages.mergeFrom(notification.getMessages());
        }
    }

    private String assembleNamespaces() {
        return STRING_JOINER.join(m_longPollNamespaces.keySet());
    }

    String assembleLongPollRefreshUrl(String uri, String appId, String cluster, String dataCenter,
                                      Map<String, Long> notificationsMap) {
        Map<String, String> queryParams = Maps.newHashMap();
        queryParams.put("appId", queryParamEscaper.escape(appId));
        queryParams.put("cluster", queryParamEscaper.escape(cluster));
        queryParams
                .put("notifications", queryParamEscaper.escape(assembleNotifications(notificationsMap)));

        if (!Strings.isNullOrEmpty(dataCenter)) {
            queryParams.put("dataCenter", queryParamEscaper.escape(dataCenter));
        }
        String localIp = m_configUtil.getLocalIp();
        if (!Strings.isNullOrEmpty(localIp)) {
            queryParams.put("ip", queryParamEscaper.escape(localIp));
        }

        String params = MAP_JOINER.join(queryParams);
        if (!uri.endsWith("/")) {
            uri += "/";
        }

        return uri + "notifications/v2?" + params;
    }

    String assembleNotifications(Map<String, Long> notificationsMap) {
        List<ApolloConfigNotification> notifications = Lists.newArrayList();
        for (Map.Entry<String, Long> entry : notificationsMap.entrySet()) {
            ApolloConfigNotification notification = new ApolloConfigNotification(entry.getKey(), entry.getValue());
            notifications.add(notification);
        }
        return gson.toJson(notifications);
    }

    private List<ServiceDTO> getConfigServices() {
        List<ServiceDTO> services = m_serviceLocator.getConfigServices();
        if (services.size() == 0) {
            throw new ApolloConfigException("No available config service");
        }

        return services;
    }
}
