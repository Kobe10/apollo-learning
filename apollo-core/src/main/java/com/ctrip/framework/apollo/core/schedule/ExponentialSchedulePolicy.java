package com.ctrip.framework.apollo.core.schedule;

/**
 * 定时策略接口。在 Apollo 中，用于执行失败，计算下一次执行的延迟时间。
 *
 * @author Jason Song(song_s@ctrip.com)
 */
public class ExponentialSchedulePolicy implements SchedulePolicy {
    /**
     * 延迟时间下限
     */
    private final long delayTimeLowerBound;
    /**
     * 延迟时间上限
     */
    private final long delayTimeUpperBound;
    /**
     * 最后延迟执行时间
     */
    private long lastDelayTime;

    public ExponentialSchedulePolicy(long delayTimeLowerBound, long delayTimeUpperBound) {
        this.delayTimeLowerBound = delayTimeLowerBound;
        this.delayTimeUpperBound = delayTimeUpperBound;
    }

    /**
     * 每次执行失败，调用 #fail() 方法，指数级计算新的延迟执行时间。
     * delayTimeLowerBound, delayTimeUpperBound= [1, 120] 执行 10 轮
     * 1 2 4 8 16 32 64 120 120 120
     * delayTimeLowerBound, delayTimeUpperBound= [30, 120] 执行 10 轮
     * 30 60 120 120 120 120 120 120 120 120 120 120
     *
     * @return
     */
    @Override
    public long fail() {
        long delayTime = lastDelayTime;
        // 设置初始时间
        if (delayTime == 0) {
            delayTime = delayTimeLowerBound;
            // 指数级计算，直到上限
        } else {
            delayTime = Math.min(lastDelayTime << 1, delayTimeUpperBound);
        }
        // 最后延迟执行时间
        lastDelayTime = delayTime;
        // 返回
        return delayTime;
    }

    @Override
    public void success() {
        lastDelayTime = 0;
    }
}
