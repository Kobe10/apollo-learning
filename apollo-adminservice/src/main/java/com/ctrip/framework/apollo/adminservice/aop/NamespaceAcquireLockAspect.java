package com.ctrip.framework.apollo.adminservice.aop;


import com.ctrip.framework.apollo.biz.config.BizConfig;
import com.ctrip.framework.apollo.biz.entity.Item;
import com.ctrip.framework.apollo.biz.entity.Namespace;
import com.ctrip.framework.apollo.biz.entity.NamespaceLock;
import com.ctrip.framework.apollo.biz.service.ItemService;
import com.ctrip.framework.apollo.biz.service.NamespaceLockService;
import com.ctrip.framework.apollo.biz.service.NamespaceService;
import com.ctrip.framework.apollo.common.dto.ItemChangeSets;
import com.ctrip.framework.apollo.common.dto.ItemDTO;
import com.ctrip.framework.apollo.common.exception.BadRequestException;
import com.ctrip.framework.apollo.common.exception.ServiceException;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Component;


/**
 * 一个namespace在一次发布中只能允许一个人修改配置  (修改可以是多个人  但是发布只能是一个人)
 * 通过数据库lock表来实现
 * 这个是锁定切面  自定义切面
 */
@Aspect
@Component
public class NamespaceAcquireLockAspect {
    private static final Logger logger = LoggerFactory.getLogger(NamespaceAcquireLockAspect.class);

    private final NamespaceLockService namespaceLockService;
    private final NamespaceService namespaceService;
    private final ItemService itemService;
    private final BizConfig bizConfig;

    public NamespaceAcquireLockAspect(
            final NamespaceLockService namespaceLockService,
            final NamespaceService namespaceService,
            final ItemService itemService,
            final BizConfig bizConfig) {
        this.namespaceLockService = namespaceLockService;
        this.namespaceService = namespaceService;
        this.itemService = itemService;
        this.bizConfig = bizConfig;
    }


    //create item  创建一条
    @Before("@annotation(PreAcquireNamespaceLock) && args(appId, clusterName, namespaceName, item, ..)")
    public void requireLockAdvice(String appId, String clusterName, String namespaceName,
                                  ItemDTO item) {
        //尝试锁定
        acquireLock(appId, clusterName, namespaceName, item.getDataChangeLastModifiedBy());
    }

    //update item  更新一条
    @Before("@annotation(PreAcquireNamespaceLock) && args(appId, clusterName, namespaceName, itemId, item, ..)")
    public void requireLockAdvice(String appId, String clusterName, String namespaceName, long itemId,
                                  ItemDTO item) {
        acquireLock(appId, clusterName, namespaceName, item.getDataChangeLastModifiedBy());
    }

    //update by change set
    @Before("@annotation(PreAcquireNamespaceLock) && args(appId, clusterName, namespaceName, changeSet, ..)")
    public void requireLockAdvice(String appId, String clusterName, String namespaceName,
                                  ItemChangeSets changeSet) {
        acquireLock(appId, clusterName, namespaceName, changeSet.getDataChangeLastModifiedBy());
    }

    //delete item
    @Before("@annotation(PreAcquireNamespaceLock) && args(itemId, operator, ..)")
    public void requireLockAdvice(long itemId, String operator) {
        // 获得 Item 对象。若不存在，抛出 BadRequestException 异常
        Item item = itemService.findOne(itemId);
        if (item == null) {
            throw new BadRequestException("item not exist.");
        }
        acquireLock(item.getNamespaceId(), operator);
    }

    void acquireLock(String appId, String clusterName, String namespaceName,
                     String currentUser) {
        // 当关闭锁定 Namespace 开关时，直接返回
        if (bizConfig.isNamespaceLockSwitchOff()) {
            return;
        }
        // 获得 Namespace 对象
        Namespace namespace = namespaceService.findOne(appId, clusterName, namespaceName);

        acquireLock(namespace, currentUser);
    }

    /**
     * 同上
     *
     * @param namespaceId
     * @param currentUser
     */
    void acquireLock(long namespaceId, String currentUser) {
        //判断是否关闭锁定 Namespace 的开关  数据库的表中设定(insert默认  更改的话需要手动修改)
        if (bizConfig.isNamespaceLockSwitchOff()) {
            return;
        }

        Namespace namespace = namespaceService.findOne(namespaceId);

        acquireLock(namespace, currentUser);

    }

    private void acquireLock(Namespace namespace, String currentUser) {
        if (namespace == null) {
            throw new BadRequestException("namespace not exist.");
        }

        long namespaceId = namespace.getId();

        NamespaceLock namespaceLock = namespaceLockService.findLock(namespaceId);
        // 当 NamespaceLock 不存在时，尝试锁定
        if (namespaceLock == null) {
            try {
                //锁定
                tryLock(namespaceId, currentUser);
                //lock success
            } catch (DataIntegrityViolationException e) {
                //lock fail
                // 锁定失败，获得 NamespaceLock 对象
                namespaceLock = namespaceLockService.findLock(namespaceId);
                // 校验锁定人是否是当前管理员
                checkLock(namespace, namespaceLock, currentUser);
            } catch (Exception e) {
                logger.error("try lock error", e);
                throw e;
            }
        } else {
            //check lock owner is current user
            checkLock(namespace, namespaceLock, currentUser);
        }
    }

    private void tryLock(long namespaceId, String user) {
        NamespaceLock lock = new NamespaceLock();
        lock.setNamespaceId(namespaceId);
        lock.setDataChangeCreatedBy(user);
        lock.setDataChangeLastModifiedBy(user);
        namespaceLockService.tryLock(lock);
    }

    private void checkLock(Namespace namespace, NamespaceLock namespaceLock,
                           String currentUser) {
        if (namespaceLock == null) {
            throw new ServiceException(
                    String.format("Check lock for %s failed, please retry.", namespace.getNamespaceName()));
        }

        String lockOwner = namespaceLock.getDataChangeCreatedBy();
        if (!lockOwner.equals(currentUser)) {
            throw new BadRequestException(
                    "namespace:" + namespace.getNamespaceName() + " is modified by " + lockOwner);
        }
    }


}
