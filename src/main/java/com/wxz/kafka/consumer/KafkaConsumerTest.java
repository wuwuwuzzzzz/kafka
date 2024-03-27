package com.wxz.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author wxz
 * @date 16:36 2024/3/27
 */
public class KafkaConsumerTest
{
    public static void main(String[] args)
    {
        // 创建配置对象
        Map<String, Object> consumerConfig = new HashMap<>(10);
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "wxz");

        // 创建消费者对象
        KafkaConsumer<String, String > consumer = new KafkaConsumer<>(consumerConfig);

        // 订阅主题
        consumer.subscribe(Collections.singletonList("test"));

        // 从 kafka 的主题中拉取数据
        ConsumerRecords<String, String> datas = consumer.poll(100);
        for (ConsumerRecord<String, String> data : datas)
        {
            System.out.println(data);
        }

        consumer.close();
    }
}
