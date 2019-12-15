package com.ctrip.framework.apollo.biz.message;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import com.ctrip.framework.apollo.biz.config.BizConfig;
import com.ctrip.framework.apollo.biz.entity.ReleaseMessage;
import com.ctrip.framework.apollo.biz.repository.ReleaseMessageRepository;
import com.ctrip.framework.apollo.core.utils.ApolloThreadFactory;
import com.ctrip.framework.apollo.tracer.Tracer;
import com.ctrip.framework.apollo.tracer.spi.Transaction;
import com.google.common.collect.Lists;

/**
 * @author Jason Song(song_s@ctrip.com)
 * 发布消息的监测类：每秒扫描一次数据库发布消息表，看是否有新的消息发布
 * 如果有新的消息发布了，就会通知所有的消息监听器 ReleaseMessageListener(包括继承了该类的类)
 * 如 NotificationControllerV2
 */
public class ReleaseMessageScanner implements InitializingBean {
    private static final Logger logger = LoggerFactory.getLogger(ReleaseMessageScanner.class);
    @Autowired
    private BizConfig bizConfig;
    @Autowired
    private ReleaseMessageRepository releaseMessageRepository;
    /**
     * 从 DB 中扫描 ReleaseMessage 表的频率，单位：毫秒
     */
    private int databaseScanInterval;
    /**
     * 监听器数组
     */
    private List<ReleaseMessageListener> listeners;
    /**
     * 定时任务服务   java8定时线程池
     */
    private ScheduledExecutorService executorService;
    /**
     * 最后扫描到的 ReleaseMessage 的编号 ---> 这个编号用来和当前的configService的版本号进行对比
     */
    private long maxIdScanned;

    public ReleaseMessageScanner() {
        // 创建监听器数组
        // 通过 #addMessageListener(ReleaseMessageListener) 方法，注册 ReleaseMessageListener 。在 MessageScannerConfiguration 中，
        // 调用该方法，初始化 ReleaseMessageScanner 的监听器们。
        listeners = Lists.newCopyOnWriteArrayList();
        //开启守护线程，单线程执行
        executorService = Executors.newScheduledThreadPool(1, ApolloThreadFactory
                .create("ReleaseMessageScanner", true));
    }

    /**
     * 通过 Spring 调用，初始化 Scan 任务
     * 这个是实现InitializingBean 的方法
     *
     * @throws Exception
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        // 从 ServerConfig 中获得频率  1000ms
        databaseScanInterval = bizConfig.releaseMessageScanIntervalInMilli();
        // 获得最大的 ReleaseMessage 的编号
        maxIdScanned = loadLargestMessageId();
        // 定时任务线程开始执行
        executorService.scheduleWithFixedDelay((Runnable) () -> {
            Transaction transaction = Tracer.newTransaction("Apollo.ReleaseMessageScanner", "scanMessage");
            try {
                // 扫描消息
                scanMessages();
                transaction.setStatus(Transaction.SUCCESS);
            } catch (Throwable ex) {
                transaction.setStatus(ex);
                logger.error("Scan and send message failed", ex);
            } finally {
                transaction.complete();
            }
        }, databaseScanInterval, databaseScanInterval, TimeUnit.MILLISECONDS);

    }

    /**
     * add message listeners for release message
     * 注册所有的监听器 在ConfigServiceAutoConfiguration进行注册
     *
     * @param listener
     */
    public void addMessageListener(ReleaseMessageListener listener) {
        if (!listeners.contains(listener)) {
            listeners.add(listener);
        }
    }

    /**
     * Scan messages, continue scanning until there is no more messages
     */
    private void scanMessages() {
        boolean hasMoreMessages = true;
        while (hasMoreMessages && !Thread.currentThread().isInterrupted()) {
            hasMoreMessages = scanAndSendMessages();
        }
    }

    /**
     * scan messages and send
     * configService扫描发布消息，并且通知到客户端
     *
     * @return whether there are more messages
     */
    private boolean scanAndSendMessages() {
        //current batch is 500
        // 获得大于 maxIdScanned 的 500 条 ReleaseMessage 记录，按照 id 升序
        List<ReleaseMessage> releaseMessages =
                releaseMessageRepository.findFirst500ByIdGreaterThanOrderByIdAsc(maxIdScanned);
        if (CollectionUtils.isEmpty(releaseMessages)) {
            return false;
        }
        // 触发监听器
        fireMessageScanned(releaseMessages);
        int messageScanned = releaseMessages.size();
        maxIdScanned = releaseMessages.get(messageScanned - 1).getId();
        return messageScanned == 500;
    }

    /**
     * find largest message id as the current start point
     *
     * @return current largest message id
     */
    private long loadLargestMessageId() {
        ReleaseMessage releaseMessage = releaseMessageRepository.findTopByOrderByIdDesc();
        return releaseMessage == null ? 0 : releaseMessage.getId();
    }

    /**
     * Notify listeners with messages loaded
     * 唤醒消息监听器，
     *
     * @param messages
     */
    private void fireMessageScanned(List<ReleaseMessage> messages) {
        for (ReleaseMessage message : messages) {
            // 这些监听器是初始化已经注册进入的
            for (ReleaseMessageListener listener : listeners) {
                try {
                    // 触发监听器
                    listener.handleMessage(message, Topics.APOLLO_RELEASE_TOPIC);
                } catch (Throwable ex) {
                    Tracer.logError(ex);
                    logger.error("Failed to invoke message listener {}", listener.getClass(), ex);
                }
            }
        }
    }
}
