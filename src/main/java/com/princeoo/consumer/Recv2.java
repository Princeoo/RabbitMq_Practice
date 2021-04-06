package com.princeoo.consumer;

import com.princeoo.util.ConnectionUtil;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * @Auther: xiaoJie
 * @Date: 2021/4/1 5:01 下午
 * @Description:
 */
public class Recv2 {

    private final static String QUEUE_NAME = "test_work_queue";

    public static void main(String[] argv) throws Exception {
        // 获取到连接
        Connection connection = ConnectionUtil.getConnection();
        //创建会话通道,生产者和mq服务所有通信都在channel通道中完成
        Channel channel = connection.createChannel();
        // 声明队列
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        //设置每个消费者同时只能处理一条信息，在手动ack下生效
        channel.basicQos(1);
        //实现消费方法
        DefaultConsumer consumer = new DefaultConsumer(channel) {
            // 获取消息，并且处理，这个方法类似事件监听，如果有消息的时候，会被自动调用
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                // body 即消息体
                String msg = new String(body, "utf-8");
                System.out.println(" [消费者2] received : " + msg + "!");
                //模拟任务耗时1s
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                // 手动进行ACK
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        };
        // 监听队列，第二个参数：true 接收到传递过来的消息后acknowledged（应答服务器），false 接收到消息后不应答服务器
        channel.basicConsume(QUEUE_NAME, false, consumer);
    }


//    public static void main(String[] argv) throws Exception {
//        // 获取到连接
//        Connection connection = ConnectionUtil.getConnection();
//        // 创建通道
//        final Channel channel = connection.createChannel();
//        // 声明队列
//        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
//        // 定义队列的消费者
//        DefaultConsumer consumer = new DefaultConsumer(channel) {
//            // 获取消息，并且处理，这个方法类似事件监听，如果有消息的时候，会被自动调用
//            @Override
//            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
////                int i = 1/0;
//                // body 即消息体
//                String msg = new String(body);
//                System.out.println(" [x] received : " + msg + "!");
//
//                /*
//                 *  void basicAck(long deliveryTag, boolean multiple) throws IOException;
//                 *  deliveryTag:用来标识消息的id
//                 *  multiple：是否批量.true:将一次性ack所有小于deliveryTag的消息。
//                 */
//                // 手动进行ACK
//                channel.basicAck(envelope.getDeliveryTag(), false);
//            }
//        };
//        // 监听队列，第二个参数false，手动进行ACK
//        channel.basicConsume(QUEUE_NAME, false, consumer);
//    }
}
