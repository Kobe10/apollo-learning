package com.ctrip.framework.apollo.core.dto;

/**
 * @author Jason Song(song_s@ctrip.com)
 * Apollo 配置通知 DTO
 */
public class ApolloConfigNotification {
    private String namespaceName;
    /**
     * 最新通知编号
     * 目前使用 `ReleaseMessage.id` 。
     */
    private long notificationId;
    /**
     * 通知消息集合
     */
    private volatile ApolloNotificationMessages messages;

    //for json converter
    public ApolloConfigNotification() {
    }

    public ApolloConfigNotification(String namespaceName, long notificationId) {
        this.namespaceName = namespaceName;
        this.notificationId = notificationId;
    }

    public String getNamespaceName() {
        return namespaceName;
    }

    public long getNotificationId() {
        return notificationId;
    }

    public void setNamespaceName(String namespaceName) {
        this.namespaceName = namespaceName;
    }

    public ApolloNotificationMessages getMessages() {
        return messages;
    }

    public void setMessages(ApolloNotificationMessages messages) {
        this.messages = messages;
    }

    //添加消息明细到 message 中。
    public void addMessage(String key, long notificationId) {
        // 创建 ApolloNotificationMessages 对象
        if (this.messages == null) {
            synchronized (this) {
                if (this.messages == null) {
                    this.messages = new ApolloNotificationMessages();
                }
            }
        }
        // 添加到 `messages` 中
        this.messages.put(key, notificationId);
    }

    @Override
    public String toString() {
        return "ApolloConfigNotification{" +
                "namespaceName='" + namespaceName + '\'' +
                ", notificationId=" + notificationId +
                '}';
    }
}
