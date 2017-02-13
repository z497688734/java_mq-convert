package com.le.user;
import org.apache.qpid.client.AMQConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Session;

/**
 * User: lichao
 * Date: 14-2-19
 * Time: 下午3:47
 */
public class SessionFactory {
    public static Logger log = LoggerFactory.getLogger(SessionFactory.class);
    private ConnectionFactory connectionFactory;
    private AMQConnection connection;
    private int maxRetrytimes = 10;
    private long retryinterval = 1000; //ms

    public Session createSession(boolean transaction, int mode) throws JMSException, InterruptedException {
        ensureConnection();
        return connection.createSession(transaction, mode);
    }

    private synchronized void ensureConnection() throws InterruptedException, JMSException {


        if (connection != null && !connection.isClosed()) {
            return;
        }

        JMSException jmse = null;
        for (int i = 1; i <= maxRetrytimes; i++) {
            try {
                connection = (AMQConnection) connectionFactory.createConnection();
                connection.start();
                return;
            } catch (JMSException e) {
                jmse = e;
                Thread.sleep(retryinterval);
                log.warn("swiftq connect is temporary failed,retry times: {}.", i);
            }
        }
        log.error("swiftq connect totally failed.");
        throw jmse;

    }

    public ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    public void setConnectionFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    public synchronized void close() throws JMSException {
        connection.close();
    }


}
