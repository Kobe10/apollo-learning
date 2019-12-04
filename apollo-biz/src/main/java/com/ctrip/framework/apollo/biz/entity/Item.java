package com.ctrip.framework.apollo.biz.entity;

import com.ctrip.framework.apollo.common.entity.BaseEntity;

import org.hibernate.annotations.SQLDelete;
import org.hibernate.annotations.Where;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Lob;
import javax.persistence.Table;

@Entity
@Table(name = "Item")
@SQLDelete(sql = "Update Item set isDeleted = 1 where id = ?")
@Where(clause = "isDeleted = 0")
public class Item extends BaseEntity {

    /**
     * Namespace 编号
     */
    @Column(name = "NamespaceId", nullable = false)
    private long namespaceId;

    /**
     * 键
     */
    @Column(name = "key", nullable = false)
    private String key;

    /**
     * 值
     * 对于 properties ，使用 Item 的 key ，对应每条配置项的键。
     * 对于 yaml 等等，使用 Item 的 key = content ，对应整个配置文件。
     */
    @Column(name = "value")
    @Lob
    private String value;

    /**
     * 注释
     */
    @Column(name = "comment")
    private String comment;

    /**
     * 行号，从一开始。
     * <p>
     * 例如 Properties 中，多个配置项。每个配置项对应一行。
     */
    @Column(name = "LineNum")
    private Integer lineNum;

    public String getComment() {
        return comment;
    }

    public String getKey() {
        return key;
    }

    public long getNamespaceId() {
        return namespaceId;
    }

    public String getValue() {
        return value;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setNamespaceId(long namespaceId) {
        this.namespaceId = namespaceId;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public Integer getLineNum() {
        return lineNum;
    }

    public void setLineNum(Integer lineNum) {
        this.lineNum = lineNum;
    }

    public String toString() {
        return toStringHelper().add("namespaceId", namespaceId).add("key", key).add("value", value)
                .add("lineNum", lineNum).add("comment", comment).toString();
    }
}
