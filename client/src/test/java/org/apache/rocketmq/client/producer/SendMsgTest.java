package org.apache.rocketmq.client.producer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.Before;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author 【张殷豪】
 * Date 2019/6/7 15:35
 */
public class SendMsgTest {
    DefaultMQProducer producer = new DefaultMQProducer("test-group");
    String nameServ = "127.0.0.1:9876";
    @Before
    public void init() throws MQClientException {
        producer.setNamesrvAddr(nameServ);
        producer.setInstanceName("rmq-instance");
        producer.start();
    }

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    @Test
    public void sendMag() throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        String topic = "topic_test";
//        for(;;){
            Thread.sleep(500);
            String format = sdf.format(new Date());
            Message message = new Message(topic,null,("hello rocket mq : "+format).getBytes());
            SendResult send = producer.send(message);
            System.out.println(send.getMsgId()+":"+send.getSendStatus());
//        }
    }

    public void close(){
        producer.shutdown();
    }


}
