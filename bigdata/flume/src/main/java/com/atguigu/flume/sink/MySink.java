package com.atguigu.flume.sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class MySink extends AbstractSink implements Configurable {
    //定义logger对象
    private final Logger logger = LoggerFactory.getLogger(MySink.class);

    private String suffix;
    private String prefix;

    @Override
    public void configure(Context context) {
        //获取前缀
        prefix = context.getString("prefix", "hello");

        //获取后缀
        suffix = context.getString("suffix");
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;

        // Start transaction
        Channel ch = getChannel();
        Transaction txn = ch.getTransaction();
        txn.begin();

        Event event = null;

        while (true) {
            event = ch.take();
            if (event != null) {
                break;
            }
        }

        try {
            logger.info(prefix + new String(event.getBody()) + suffix);

            txn.commit();
            status = Status.READY;
        } catch (Throwable t) {
            txn.rollback();

            // Log exception, handle individual exceptions as needed

            status = Status.BACKOFF;

            // re-throw all Errors
            if (t instanceof Error) {
                throw (Error) t;
            }
        } finally {
            txn.close();
        }
        return status;
    }
}
