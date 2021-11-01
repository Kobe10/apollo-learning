package com.ctrip.framework.apollo.internals;

import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.framework.apollo.tracer.Tracer;
import com.ctrip.framework.apollo.util.ExceptionUtil;
import com.google.common.collect.Lists;

/**
 * 抽象配置数据库操作类
 *
 * @author Jason Song(song_s@ctrip.com)
 */
public abstract class AbstractConfigRepository implements ConfigRepository {
    private static final Logger logger = LoggerFactory.getLogger(AbstractConfigRepository.class);
    /**
     * RepositoryChangeListener 数组  ---CopyOnWriteArrayList  读写安全的list
     */
    private List<RepositoryChangeListener> m_listeners = Lists.newCopyOnWriteArrayList();

    /**
     * 尝试同步操作
     *
     * @return
     */
    protected boolean trySync() {
        try {
            sync();
            return true;
        } catch (Throwable ex) {
            Tracer.logEvent("ApolloConfigException", ExceptionUtil.getDetailMessage(ex));
            logger
                    .warn("Sync config failed, will retry. Repository {}, reason: {}", this.getClass(), ExceptionUtil
                            .getDetailMessage(ex));
        }
        return false;
    }

    /**
     * 同步 ：抽象方法--子类实现
     */
    protected abstract void sync();

    @Override
    public void addChangeListener(RepositoryChangeListener listener) {
        if (!m_listeners.contains(listener)) {
            m_listeners.add(listener);
        }
    }

    @Override
    public void removeChangeListener(RepositoryChangeListener listener) {
        m_listeners.remove(listener);
    }

    /**
     * 触发监听器
     *
     * @param namespace
     * @param newProperties
     */
    protected void fireRepositoryChange(String namespace, Properties newProperties) {
        // 循环 RepositoryChangeListener 数组
        for (RepositoryChangeListener listener : m_listeners) {
            try {
                // 触发监听器
                listener.onRepositoryChange(namespace, newProperties);
            } catch (Throwable ex) {
                Tracer.logError(ex);
                logger.error("Failed to invoke repository change listener {}", listener.getClass(), ex);
            }
        }
    }
}
