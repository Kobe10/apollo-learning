package com.ctrip.framework.apollo.biz.service;

import com.ctrip.framework.apollo.biz.entity.NamespaceLock;
import com.ctrip.framework.apollo.biz.repository.NamespaceLockRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class NamespaceLockService {

    private final NamespaceLockRepository namespaceLockRepository;

    public NamespaceLockService(final NamespaceLockRepository namespaceLockRepository) {
        this.namespaceLockRepository = namespaceLockRepository;
    }

    /**
     * 查询是否有锁定namespace
     *
     * @param namespaceId
     * @return
     */
    public NamespaceLock findLock(Long namespaceId) {
        return namespaceLockRepository.findByNamespaceId(namespaceId);
    }

    /**
     * 给某个namespace加锁
     *
     * @param lock
     * @return
     */
    @Transactional
    public NamespaceLock tryLock(NamespaceLock lock) {
        return namespaceLockRepository.save(lock);
    }

    /**
     * 给某个namespace解锁
     *
     * @param namespaceId
     * @return
     */
    @Transactional
    public void unlock(Long namespaceId) {
        namespaceLockRepository.deleteByNamespaceId(namespaceId);
    }
}
