package com.atguigu.flume.source;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

public class MySource extends AbstractSource implements Configurable, PollableSource {
    private String prefix;
    private Integer delay;

    @Override
    public void configure(Context context) {
        //获取前缀
        prefix = context.getString("prefix");
        delay = context.getInteger("delay");
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status = Status.BACKOFF;
        try {
            for (int i = 0; i < 5; i++) {
                //封装数据
                Event event = new SimpleEvent();
                event.setBody((prefix + i).getBytes());

                //把数据给到ChannelProcessor
                getChannelProcessor().processEvent(event);
                Thread.sleep(delay);
            }

            //修改状态
            status = Status.READY;
        } catch (Throwable t) {
            //修改状态
            status = Status.BACKOFF;

            if (t instanceof Error) {
                throw (Error)t;
            }
        }
        return status;
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }
}
