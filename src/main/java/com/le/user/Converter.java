package com.le.user;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
/** 
 * @ClassName: Consumer 
 * @Description: TODO 
 * @author anyunzhong 
 * @date 2014-5-21 下午1:41:28 
 *  
 */
import java.util.List;
import java.util.Map;

import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.qpid.client.AMQAnyDestination;
import org.apache.qpid.client.AMQConnectionFactory;
import org.apache.qpid.url.URLSyntaxException;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.le.user.SessionFactory;

public class Converter {
	private static Session _session;
	private static MessageProducer _producer;

    /**
     * 当前例子是PushConsumer用法，使用方式给用户感觉是消息从RocketMQ服务器推到了应用客户端。<br>
     * 但是实际PushConsumer内部是使用长轮询Pull方式从Broker拉消息，然后再回调用户Listener方法<br>
     * @throws JMSException 
     * @throws URISyntaxException 
     */
    public static void main(String[] args) throws InterruptedException,
            MQClientException, JMSException, URISyntaxException {
        /**
         * 一个应用创建一个Consumer，由应用来维护此对象，可以设置为全局对象或者单例<br>
         * 注意：ConsumerGroupName需要由应用来保证唯一
         */
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(
                "ConsumerGroupName");

        consumer.setNamesrvAddr("10.154.250.5:9876");  
        /**
         * 订阅指定topic下tags分别等于TagA或TagC或TagD
         */
        consumer.subscribe("TopicTest1", "TagA || TagC || TagD");
        /**
         * 订阅指定topic下所有消息<br>
         * 注意：一个consumer对象可以订阅多个topic
         */
        consumer.subscribe("TopicTest2", "*");

        /**
         * 设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费<br>
         * 如果非第一次启动，那么按照上次消费的位置继续消费
         */
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        
        
        //创建swiftQ
        AMQConnectionFactory connectionFactory = new AMQConnectionFactory();
        //connectionFactory.setConnectionURLString("amqp://admin:admin@/?brokerlist='tcp://10.11.144.92:5672'&sync_publish='all'&sync_ack=true");
        connectionFactory.setConnectionURLString("amqp://*:*@/?brokerlist='tcp://10.11.144.92:5672'&sync_publish='all'&sync_ack=true");
        SessionFactory sessionFactory = new SessionFactory();
        sessionFactory.setConnectionFactory(connectionFactory);
        Converter._session = sessionFactory.createSession(/*是否支持事务*/false, Session.AUTO_ACKNOWLEDGE);
        
        AMQAnyDestination topic = new AMQAnyDestination("leme.base_misc.queue");
        Converter._producer = Converter._session.createProducer(topic);
        Converter._producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        //

        consumer.registerMessageListener(new MessageListenerConcurrently() {

            /**
             * * 默认msgs里只有一条消息，可以通过设置consumeMessageBatchMaxSize参数来批量接收消息
             */
            public ConsumeConcurrentlyStatus consumeMessage(
                    List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                System.out.println(Thread.currentThread().getName()
                        + " Receive New Messages: " + msgs);

                MessageExt msg = msgs.get(0);
               // System.out.println(new String(msg.getBody()));
                
                //String strMsg = new String(msg.getBody());
                try {
					TextMessage txtMsg  = Converter._session.createTextMessage(new String(msg.getBody()));
					Converter._producer.send(txtMsg);
					System.out.println("send suss;msg is : "+new String(msg.getBody()));
					

				} catch (JMSException e) {
					System.out.println("send fail..................");
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
                
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        /**
         * Consumer对象在使用之前必须要调用start初始化，初始化一次即可<br>
         */
        consumer.start();

        System.out.println("Consumer Started.");
    }
    
    
    private MapMessage createMsg(Session session) throws JMSException{
        //只发送字符串
        //TextMessage tmsg = session.createTextMessage(new String(msg));
        MapMessage msg = session.createMapMessage();
        msg.setIntProperty("id", 987654321);
        msg.setStringProperty("name", "demo");
        msg.setDoubleProperty("price", 0.6);
         
        List<String> colors = new ArrayList<String>();
        colors.add("red");
        colors.add("green");
        colors.add("white");       
        msg.setObject("colours", colors);
         
        Map<String, Double> dimensions = new HashMap<String, Double>();
        dimensions.put("length", 3.0);
        dimensions.put("width", 4.0);
        dimensions.put("depth", 5.0);
        msg.setObject("dimensions",dimensions);
         
        return msg;
    }
}