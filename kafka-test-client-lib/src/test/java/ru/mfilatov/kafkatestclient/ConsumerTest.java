package ru.mfilatov.kafkatestclient;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import ru.mfilatov.kafkatestclient.annotations.KafkaReader;
import ru.mfilatov.kafkatestclient.annotations.KafkaWriter;
import ru.mfilatov.kafkatestclient.config.KafkaFileConfigProvider;
import ru.mfilatov.kafkatestclient.model.KafkaMessage;
import ru.mfilatov.kafkatestclient.subscriber.KafkaMessageSubscriber;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Slf4j
@KafkaReader("test")
@KafkaWriter("test")
public class ConsumerTest extends AbstractBaseTest{
    @Test
    @SneakyThrows
    void consumeMessageTest(){
        var messages = new ArrayList<KafkaMessage>();
        for(int i = 0; i< 10; i++) {
            var message = KafkaMessage.builder().key(UUID.randomUUID().toString()).value(UUID.randomUUID().toString())
                    .headers(Map.of("RqUID", UUID.randomUUID().toString())).build();
            messages.add(message);
        }
        TimeUnit.SECONDS.sleep(15);

        kafka.send(messages);

        TimeUnit.SECONDS.sleep(15);
    }
}
