package com.ctrip.framework.apollo.biz.entity;

import com.ctrip.framework.apollo.common.entity.BaseEntity;

import org.hibernate.annotations.Where;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;

@Entity
@Table(name = "NamespaceLock")
@Where(clause = "isDeleted = 0")
/**
 * 写操作 Item 时，创建 Namespace 对应的 NamespaceLock 记录到 ConfigDB 数据库中，从而记录配置修改人。
 * 该字段上有唯一索引。通过该锁定，保证并发写操作时，同一个 Namespace 有且仅有创建一条 NamespaceLock 记录。
 * 并发写操作：当修改namespace时，在这张表里面添加一条记录，表示锁定，每次修改namespace时都会查询这张表
 */
public class NamespaceLock extends BaseEntity {

    /**
     * Namespace 编号 {@link Namespace}
     * <p>
     * 唯一索引
     */
    @Column(name = "NamespaceId")
    private long namespaceId;

    public long getNamespaceId() {
        return namespaceId;
    }

    public void setNamespaceId(long namespaceId) {
        this.namespaceId = namespaceId;
    }
}
