package com.ctrip.framework.apollo.configservice.service.config;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;

import com.ctrip.framework.apollo.biz.entity.Release;
import com.ctrip.framework.apollo.biz.entity.ReleaseMessage;
import com.ctrip.framework.apollo.biz.message.Topics;
import com.ctrip.framework.apollo.biz.service.ReleaseMessageService;
import com.ctrip.framework.apollo.biz.service.ReleaseService;
import com.ctrip.framework.apollo.biz.utils.ReleaseMessageKeyGenerator;
import com.ctrip.framework.apollo.core.ConfigConsts;
import com.ctrip.framework.apollo.core.dto.ApolloNotificationMessages;
import com.ctrip.framework.apollo.tracer.Tracer;
import com.ctrip.framework.apollo.tracer.spi.Transaction;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

/**
 * config service with guava cache(使用guava的缓存机制)
 *
 * @author Jason Song(song_s@ctrip.com)
 */
public class ConfigServiceWithCache extends AbstractConfigService {
    private static final Logger logger = LoggerFactory.getLogger(ConfigServiceWithCache.class);
    /**
     * 默认缓存过滤时间，单位：分钟
     */
    private static final long DEFAULT_EXPIRED_AFTER_ACCESS_IN_MINUTES = 60;//1 hour
    // TRACER 日志内存的枚举
    private static final String TRACER_EVENT_CACHE_INVALIDATE = "ConfigCache.Invalidate";
    private static final String TRACER_EVENT_CACHE_LOAD = "ConfigCache.LoadFromDB";
    private static final String TRACER_EVENT_CACHE_LOAD_ID = "ConfigCache.LoadFromDBById";
    private static final String TRACER_EVENT_CACHE_GET = "ConfigCache.Get";
    private static final String TRACER_EVENT_CACHE_GET_ID = "ConfigCache.GetById";
    private static final Splitter STRING_SPLITTER =
            Splitter.on(ConfigConsts.CLUSTER_NAMESPACE_SEPARATOR).omitEmptyStrings();

    @Autowired
    private ReleaseService releaseService;

    @Autowired
    private ReleaseMessageService releaseMessageService;

    /**
     * ConfigCacheEntry 缓存
     * <p>
     * KEY：Watch Key {@link ReleaseMessage}
     */
    private LoadingCache<String, ConfigCacheEntry> configCache;

    /**
     * Release 缓存
     * <p>
     * KEY ：Release 编号
     */
    private LoadingCache<Long, Optional<Release>> configIdCache;
    /**
     * 无 ConfigCacheEntry 占位对象
     */
    private ConfigCacheEntry nullConfigCacheEntry;

    public ConfigServiceWithCache() {
        nullConfigCacheEntry = new ConfigCacheEntry(ConfigConsts.NOTIFICATION_ID_PLACEHOLDER, null);
    }

    /**
     * 通过 Spring 调用，初始化缓存对象
     */
    @PostConstruct
    void initialize() {
        /**
         * 配置实体缓存初始化   guava cache书写方式
         */
        configCache = CacheBuilder.newBuilder()
                //实效时间
                .expireAfterAccess(DEFAULT_EXPIRED_AFTER_ACCESS_IN_MINUTES, TimeUnit.MINUTES)
                .build(new CacheLoader<String, ConfigCacheEntry>() {
                    @Override
                    public ConfigCacheEntry load(String key) throws Exception {
                        // 判断key的格式不正确，返回 nullConfigCacheEntry
                        List<String> namespaceInfo = STRING_SPLITTER.splitToList(key);
                        if (namespaceInfo.size() != 3) {
                            Tracer.logError(
                                    new IllegalArgumentException(String.format("Invalid cache load key %s", key)));
                            return nullConfigCacheEntry;
                        }

                        Transaction transaction = Tracer.newTransaction(TRACER_EVENT_CACHE_LOAD, key);
                        try {
                            //// 获得最新的 ReleaseMessage 对象
                            ReleaseMessage latestReleaseMessage = releaseMessageService.findLatestReleaseMessageForMessages(Lists
                                    .newArrayList(key));
                            // 获得最新的，并且有效的 Release 对象
                            Release latestRelease = releaseService.findLatestActiveRelease(namespaceInfo.get(0), namespaceInfo.get(1),
                                    namespaceInfo.get(2));

                            transaction.setStatus(Transaction.SUCCESS);
                            // 获得通知编号
                            long notificationId = latestReleaseMessage == null ? ConfigConsts.NOTIFICATION_ID_PLACEHOLDER : latestReleaseMessage
                                    .getId();
                            // 若 latestReleaseMessage 和 latestRelease 都为空，返回 nullConfigCacheEntry
                            if (notificationId == ConfigConsts.NOTIFICATION_ID_PLACEHOLDER && latestRelease == null) {
                                return nullConfigCacheEntry;
                            }
                            // 创建 ConfigCacheEntry 对象
                            return new ConfigCacheEntry(notificationId, latestRelease);
                        } catch (Throwable ex) {
                            transaction.setStatus(ex);
                            throw ex;
                        } finally {
                            transaction.complete();
                        }
                    }
                });
        //初始化cache
        configIdCache = CacheBuilder.newBuilder()
                .expireAfterAccess(DEFAULT_EXPIRED_AFTER_ACCESS_IN_MINUTES, TimeUnit.MINUTES)
                .build(new CacheLoader<Long, Optional<Release>>() {
                    @Override
                    public Optional<Release> load(Long key) throws Exception {
                        Transaction transaction = Tracer.newTransaction(TRACER_EVENT_CACHE_LOAD_ID, String.valueOf(key));
                        try {
                            Release release = releaseService.findActiveOne(key);

                            transaction.setStatus(Transaction.SUCCESS);

                            return Optional.ofNullable(release);
                        } catch (Throwable ex) {
                            transaction.setStatus(ex);
                            throw ex;
                        } finally {
                            transaction.complete();
                        }
                    }
                });
    }

    @Override
    protected Release findActiveOne(long id, ApolloNotificationMessages clientMessages) {
        Tracer.logEvent(TRACER_EVENT_CACHE_GET_ID, String.valueOf(id));
        return configIdCache.getUnchecked(id).orElse(null);
    }

    @Override
    protected Release findLatestActiveRelease(String appId, String clusterName, String namespaceName,
                                              ApolloNotificationMessages clientMessages) {
        // 根据 appId + clusterName + namespaceName ，获得 ReleaseMessage 的 `message`
        String key = ReleaseMessageKeyGenerator.generate(appId, clusterName, namespaceName);

        Tracer.logEvent(TRACER_EVENT_CACHE_GET, key);
        // 从缓存 configCache 中，读取 ConfigCacheEntry 对象
        ConfigCacheEntry cacheEntry = configCache.getUnchecked(key);

        // 若客户端的通知编号更大，说明缓存已经过期。
        //cache is out-dated
        if (clientMessages != null && clientMessages.has(key) &&
                clientMessages.get(key) > cacheEntry.getNotificationId()) {
            //invalidate the cache and try to load from db again
            invalidate(key);
            cacheEntry = configCache.getUnchecked(key);
        }

        return cacheEntry.getRelease();
    }

    private void invalidate(String key) {
        // 删除缓存
        configCache.invalidate(key);
        Tracer.logEvent(TRACER_EVENT_CACHE_INVALIDATE, key);
    }

    /**
     * 仅处理 APOLLO_RELEASE_TOPICl 。
     *
     * @param message
     * @param channel 通道（主题）
     */
    @Override
    public void handleMessage(ReleaseMessage message, String channel) {
        logger.info("message received - channel: {}, message: {}", channel, message);
        if (!Topics.APOLLO_RELEASE_TOPIC.equals(channel) || Strings.isNullOrEmpty(message.getMessage())) {
            return;
        }

        try {
            // 清空对应的缓存
            invalidate(message.getMessage());
            // 预热缓存，读取 ConfigCacheEntry 对象，重新从 DB 中加载。
            //调用 LoadingCache#getUnchecked(key) 方法，预热缓存，读取 ConfigCacheEntry 对象，重新从 DB 中加载。
            // getUnchecked(key)  查询缓存
            configCache.getUnchecked(message.getMessage());
        } catch (Throwable ex) {
            //ignore
        }
    }

    /**
     * 配置缓存 Entry
     */
    private static class ConfigCacheEntry {
        /**
         * 通知编号
         */
        private final long notificationId;
        /**
         * Release 对象
         */
        private final Release release;

        public ConfigCacheEntry(long notificationId, Release release) {
            this.notificationId = notificationId;
            this.release = release;
        }

        public long getNotificationId() {
            return notificationId;
        }

        public Release getRelease() {
            return release;
        }
    }
}
