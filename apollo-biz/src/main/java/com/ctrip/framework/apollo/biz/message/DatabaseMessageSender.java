package com.ctrip.framework.apollo.biz.message;

import com.ctrip.framework.apollo.biz.entity.ReleaseMessage;
import com.ctrip.framework.apollo.biz.repository.ReleaseMessageRepository;
import com.ctrip.framework.apollo.core.utils.ApolloThreadFactory;
import com.ctrip.framework.apollo.tracer.Tracer;
import com.ctrip.framework.apollo.tracer.spi.Transaction;
import com.google.common.collect.Queues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Jason Song(song_s@ctrip.com)
 * Message 发送者实现类，基于数据库实现
 */
@Component
public class DatabaseMessageSender implements MessageSender {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseMessageSender.class);
    /**
     * 清理 Message 队列 最大容量
     */
    private static final int CLEAN_QUEUE_MAX_SIZE = 100;
    /**
     * 清理 Message 队列  guava 工具包:简化代码开发
     */
    private BlockingQueue<Long> toClean = Queues.newLinkedBlockingQueue(CLEAN_QUEUE_MAX_SIZE);
    /**
     * 清理 Message ExecutorService
     */
    private final ExecutorService cleanExecutorService;
    /**
     * 是否停止清理 Message 标识
     */
    private final AtomicBoolean cleanStopped;

    private final ReleaseMessageRepository releaseMessageRepository;

    public DatabaseMessageSender(final ReleaseMessageRepository releaseMessageRepository) {
        // 创建 ExecutorService 对象
        cleanExecutorService = Executors.newSingleThreadExecutor(ApolloThreadFactory.create("DatabaseMessageSender", true));
        cleanStopped = new AtomicBoolean(false);
        this.releaseMessageRepository = releaseMessageRepository;
    }

    @Override
    @Transactional
    public void sendMessage(String message, String channel) {
        logger.info("Sending message {} to channel {}", message, channel);
        // 仅允许发送 APOLLO_RELEASE_TOPIC 这个单独的topic
        if (!Objects.equals(channel, Topics.APOLLO_RELEASE_TOPIC)) {
            logger.warn("Channel {} not supported by DatabaseMessageSender!");
            return;
        }

        Tracer.logEvent("Apollo.AdminService.ReleaseMessage", message);
        Transaction transaction = Tracer.newTransaction("Apollo.AdminService", "sendMessage");
        try {
            // 保存 ReleaseMessage 对象   保存发送结果(configService扫描器会去轮询扫描这条结果)
            ReleaseMessage newMessage = releaseMessageRepository.save(new ReleaseMessage(message));
            // 将发布结果加入到清除队列当中： 添加到清理 Message 队列。若队列已满，添加失败，不阻塞等待。
            toClean.offer(newMessage.getId());
            transaction.setStatus(Transaction.SUCCESS);
        } catch (Throwable ex) {
            logger.error("Sending message to database failed", ex);
            transaction.setStatus(ex);
            throw ex;
        } finally {
            transaction.complete();
        }
    }

    /**
     * 清理 ReleaseMessage 任务：通知 Spring 调用，初始化清理 ReleaseMessage 任务
     * 多线程执行清理
     */
    @PostConstruct
    private void initialize() {
        cleanExecutorService.submit(() -> {
            // 若未停止，持续运行
            while (!cleanStopped.get() && !Thread.currentThread().isInterrupted()) {
                try {
                    //每隔1s 去blockingQueue中获取清除数据，指定时间
                    Long rm = toClean.poll(1, TimeUnit.SECONDS);
                    if (rm != null) {
                        //清除数据
                        cleanMessage(rm);
                    } else {
                        //如果没有数据，当前线程阻塞5s进行等待；避免空跑，占用 CPU
                        TimeUnit.SECONDS.sleep(5);
                    }
                } catch (Throwable ex) {
                    Tracer.logError(ex);
                }
            }
        });
    }

    private void cleanMessage(Long id) {
        boolean hasMore = true;
        //double check in case the release message is rolled back
        ReleaseMessage releaseMessage = releaseMessageRepository.findById(id).orElse(null);
        if (releaseMessage == null) {
            return;
        }
        while (hasMore && !Thread.currentThread().isInterrupted()) {
            List<ReleaseMessage> messages = releaseMessageRepository.findFirst100ByMessageAndIdLessThanOrderByIdAsc(
                    releaseMessage.getMessage(), releaseMessage.getId());

            releaseMessageRepository.deleteAll(messages);
            hasMore = messages.size() == 100;

            messages.forEach(toRemove -> Tracer.logEvent(
                    String.format("ReleaseMessage.Clean.%s", toRemove.getMessage()), String.valueOf(toRemove.getId())));
        }
    }

    void stopClean() {
        cleanStopped.set(true);
    }
}
