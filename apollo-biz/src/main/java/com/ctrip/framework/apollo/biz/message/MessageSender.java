package com.ctrip.framework.apollo.biz.message;

/**
 * @author Jason Song(song_s@ctrip.com)
 * 信息发送接口
 */
public interface MessageSender {
    void sendMessage(String message, String channel);
}
