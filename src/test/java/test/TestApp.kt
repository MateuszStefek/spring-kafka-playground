package test

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.kafka.test.EmbeddedKafkaBroker
import org.springframework.messaging.handler.annotation.Header
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.scheduling.annotation.Scheduled

@SpringBootApplication
@EnableScheduling
open class TestApp(
        private val kafkaTemplate: KafkaTemplate<String, String>,
        private val kafkaProperties: KafkaProperties?
) {
    companion object {
        const val TEST_TOPIC = "test-topic"
        const val GROUP = "g"

        @JvmStatic
        fun main(args: Array<String>) {
            SpringApplication.run(TestApp::class.java)
        }
    }

    @KafkaListener(topics = [TEST_TOPIC], groupId = GROUP)
    open fun onMessage(messages: List<String>,
                       @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) keys: List<String>) {
        println("Messages. keys = $keys messages $messages")
    }

    @Bean
    open fun embeddedKafka(): EmbeddedKafkaBroker = EmbeddedKafkaBroker(1,
            false,
            1,
            TEST_TOPIC).kafkaPorts(9092)

    @Bean
    open fun kafkaConsumerFactory(): ConsumerFactory<Any, Any> {
        val cc = this.kafkaProperties!!.buildConsumerProperties()
        //cc.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 500);
        cc.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 100)
        cc.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 120)
        cc.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 50)
        return DefaultKafkaConsumerFactory(
                cc)
    }

    @Bean
    open fun kafkaListenerContainerFactory(
            configurer: ConcurrentKafkaListenerContainerFactoryConfigurer): ConcurrentKafkaListenerContainerFactory<*, *> {
        val factory = ConcurrentKafkaListenerContainerFactory<Any, Any>()
        configurer.configure(factory, kafkaConsumerFactory())
        factory.isBatchListener = true
        return factory
    }

    @Scheduled(fixedRate = 100)
    fun x() {
        for (i in 1 until 10) {
            kafkaTemplate!!.send(TEST_TOPIC, "key${i % 2}", "data$i")
        }
    }


}
