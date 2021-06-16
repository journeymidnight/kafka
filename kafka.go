package kafka

import (
	"errors"
	"github.com/Shopify/sarama"
	"math/rand"
	"time"
)

const DefaultProducerMessageMaxBytes = 10 << 20 // 10M

func saramaConfigWithDefault() *sarama.Config {
	c := sarama.NewConfig()

	c.Version = sarama.MaxVersion // TODO need to stick to some version?
	c.ClientID = "yig"

	c.Net.DialTimeout = 3 * time.Second
	c.Net.ReadTimeout = 20 * time.Second
	c.Net.WriteTimeout = 20 * time.Second
	c.Net.KeepAlive = time.Minute

	c.Producer.MaxMessageBytes = DefaultProducerMessageMaxBytes
	c.Producer.RequiredAcks = sarama.WaitForAll

	c.Consumer.Fetch.Default = 10 << 20 // 10M
	c.Consumer.MaxProcessingTime = 10 * time.Second
	c.Consumer.Return.Errors = true

	return c
}

type Producer struct {
	topic    string
	producer sarama.AsyncProducer
}

func NewProducer(addresses []string, topic string) (Producer, error) {
	if len(topic) == 0 {
		return Producer{}, errors.New("empty topic")
	}

	kafkaConfig := saramaConfigWithDefault()

	producer, err := sarama.NewAsyncProducer(
		addresses, kafkaConfig)
	if err != nil {
		panic("Create kafka producer error: " + err.Error())
	}
	p := Producer{
		topic:    topic,
		producer: producer,
	}
	return p, nil
}

// Publish publishes message
func (p Producer) Publish(partitionKey string, message []byte) {
	producerMessage := &sarama.ProducerMessage{
		Topic: p.topic,
		Key:   sarama.StringEncoder(partitionKey),
		Value: sarama.ByteEncoder(message),
	}
	if len(partitionKey) == 0 {
		key := make([]byte, 4, 4)
		rand.Read(key)
		producerMessage.Key = sarama.ByteEncoder(key)
	}
	p.producer.Input() <- producerMessage
}

// Simplify republishing messages from error channel
func (p Producer) Republish(msg *sarama.ProducerMessage) {
	p.producer.Input() <- msg
}

// Get producer error channel
func (p Producer) Errors() <-chan *sarama.ProducerError {
	return p.producer.Errors()
}

// Close the producer
func (p Producer) Close() {
	_ = p.producer.Close()
}

func NewConsumer(addresses []string) (sarama.Consumer, error) {
	kafkaConfig := saramaConfigWithDefault()
	return sarama.NewConsumer(addresses, kafkaConfig)
}

type Consumer struct {
	topic             string
	consumer          sarama.Consumer
	partitionConsumer sarama.PartitionConsumer
}

func NewPartitionConsumer(addresses []string, topic string,
	partition int32, offset int64) (Consumer, error) {

	consumer, err := NewConsumer(addresses)
	if err != nil {
		return Consumer{}, err
	}
	partitionConsumer, err := consumer.ConsumePartition(topic, partition, offset)
	if err == sarama.ErrOffsetOutOfRange {
		partitionConsumer, err = consumer.ConsumePartition(
			topic, partition, sarama.OffsetNewest)
	}
	if err != nil {
		return Consumer{}, err
	}
	c := Consumer{
		topic:             topic,
		consumer:          consumer,
		partitionConsumer: partitionConsumer,
	}
	return c, nil
}

// Consume a single message, block if no message in queue
func (c Consumer) Messages() <-chan *sarama.ConsumerMessage {
	return c.partitionConsumer.Messages()
}

// Get consumer error channel
func (c Consumer) Errors() <-chan *sarama.ConsumerError {
	return c.partitionConsumer.Errors()
}

// Close the producer
func (c Consumer) Close() {
	_ = c.partitionConsumer.Close()
	_ = c.consumer.Close()
}
