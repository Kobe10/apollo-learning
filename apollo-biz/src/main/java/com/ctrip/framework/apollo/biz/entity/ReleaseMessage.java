package com.ctrip.framework.apollo.biz.entity;

import com.google.common.base.MoreObjects;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.PrePersist;
import javax.persistence.Table;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
@Entity
@Table(name = "ReleaseMessage")
public class ReleaseMessage {
    /**
     * 因此，对于同一个 Namespace ，生成的消息内容是相同的。通过这样的方式，我们可以使用最新的 ReleaseMessage 的 id 属性，
     * 作为 Namespace 是否发生变更的标识。而 Apollo 确实是通过这样的方式实现，Client 通过不断使用获得到 ReleaseMessage 的
     * id 属性作为版本号，请求 Config Service 判断是否配置发生了变化。
     */
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "Id")
    private long id;

    /**
     * 消息内容，通过 {@link com.ctrip.framework.apollo.biz.utils.ReleaseMessageKeyGenerator#generate(String, String, String)} 方法生成。
     */
    @Column(name = "Message", nullable = false)
    private String message;

    @Column(name = "DataChange_LastTime")
    private Date dataChangeLastModifiedTime;

    /**
     * 前置操作
     */
    @PrePersist
    protected void prePersist() {
        if (this.dataChangeLastModifiedTime == null) {
            dataChangeLastModifiedTime = new Date();
        }
    }

    public ReleaseMessage() {
    }

    public ReleaseMessage(String message) {
        this.message = message;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .omitNullValues()
                .add("id", id)
                .add("message", message)
                .add("dataChangeLastModifiedTime", dataChangeLastModifiedTime)
                .toString();
    }
}
