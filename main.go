package main

import (
	"bytes"
	"fmt"
	"github.com/Shopify/sarama"
	"langolier/env"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

var (
	processTmpDir string
)

func main() {
	var err error

	log.SetOutput(os.Stdout)
	log.SetFlags(log.LUTC | log.Lmicroseconds | log.Ltime)

	var (
		topic                     string
		topicPartition            int
		topicPartitionOffsetStart int64
		topicPartitionOffsetStop  int64
	)

	topic = env.GetOrFail("TOPIC")
	topicPartition, _ = strconv.Atoi(env.GetOrFail("TOPIC_PARTITION"))
	topicPartitionOffsetStart, _ = strconv.ParseInt(env.GetOrDefault("TOPIC_PARTITION_OFFSET_START", "0"), 10, 64)
	topicPartitionOffsetStop, _ = strconv.ParseInt(env.GetOrDefault("TOPIC_PARTITION_OFFSET_STOP", "-1"), 10, 64)

	headerName := env.GetOrDefault("HEADER_NAME", "tenantId")
	headerValues := strings.Split(env.GetOrFail("HEADER_VALUES"), ",")

	headerNameBytes := []byte(headerName)
	headerValuesBytes := make([][]byte, len(headerValues))
	for i := range headerValues {
		headerValuesBytes[i] = []byte(headerValues[i])
	}

	sarama.Logger = log.New(os.Stderr, "sarama: ", log.LUTC|log.Lmicroseconds|log.Ltime|log.Lmsgprefix)

	processTmpDir = env.GetOrSupply("KAFKA_TMP", func() string {
		return os.TempDir()
	})
	processTmpDir = filepath.Join(processTmpDir, strconv.Itoa(os.Getpid()))
	err = os.MkdirAll(processTmpDir, 0700)
	if err != nil {
		log.Fatalln(err)
		return
	}

	var addrs []string
	var conf *sarama.Config
	addrs, conf, err = createKafkaClient()
	if err != nil {
		log.Fatalln(err)
	}

	conf.Producer.Return.Successes = true
	conf.Producer.Return.Errors = true
	// use the raw Partition value in ProducerMessage
	conf.Producer.Partitioner = sarama.NewManualPartitioner

	var client sarama.Client
	client, err = sarama.NewClient(addrs, conf)
	if err != nil {
		log.Fatalln(err)
	}

	defer func() {
		err := client.Close()
		if err != nil {
			log.Println(err)
		}
	}()

	var prod sarama.SyncProducer
	prod, err = sarama.NewSyncProducerFromClient(client)
	if err != nil {
		log.Fatalln(err)
	}

	var cons sarama.Consumer
	cons, err = sarama.NewConsumerFromClient(client)
	if err != nil {
		log.Fatalln(err)
	}

	// consume a partition from start to end:
	var partitionConsumer sarama.PartitionConsumer
	partitionConsumer, err = cons.ConsumePartition(topic, int32(topicPartition), topicPartitionOffsetStart)
	if err != nil {
		return
	}

	log.Printf(
		"consuming topic '%s' partition %d from offset %7d to %7d (inclusive)",
		topic,
		topicPartition,
		topicPartitionOffsetStart,
		topicPartitionOffsetStop,
	)
	for message := range partitionConsumer.Messages() {
		var match bool
		var partition int32
		var offset int64
		var outputHeaders []sarama.RecordHeader

		partition = message.Partition
		offset = message.Offset

		prefix := fmt.Sprintf("[%s][%3d][%7d]", topic, partition, offset)

		// stop is inclusive, so only check if we've consumed an offset greater than it:
		if offset > topicPartitionOffsetStop {
			log.Printf("%s: stopping because consuming next message would exceed stop offset %7d\n", prefix, topicPartitionOffsetStop)
			break
		}

		// skip existing tombstones:
		if message.Value == nil {
			log.Printf("%s: skipping message for key='%s' because tombstone...\n", prefix, message.Key)
			goto checkOffset
		}
		if len(message.Value) == 0 {
			log.Printf("%s: skipping message for key='%s' because tombstone...\n", prefix, message.Key)
			goto checkOffset
		}
		if len(message.Headers) == 0 {
			log.Printf("%s: skipping message for key='%s' because no headers found...\n", prefix, message.Key)
			goto checkOffset
		}

		// check header values:
		match = false
	findMatch:
		for _, h := range message.Headers {
			// check header key:
			if bytes.Compare(h.Key, headerNameBytes) != 0 {
				continue
			}

			// check header value:
			for _, v := range headerValuesBytes {
				if bytes.Compare(h.Value, v) == 0 {
					match = true
					log.Printf("%s: matched key='%s' on header %s='%s'\n", prefix, message.Key, h.Key, h.Value)
					break findMatch
				}
			}
		}

		if !match {
			log.Printf("%s: skipping message for key='%s' because no header matches...\n", prefix, message.Key)
			goto checkOffset
		}

		// copy headers:
		outputHeaders = make([]sarama.RecordHeader, len(message.Headers))
		for i := range message.Headers {
			outputHeaders[i] = *message.Headers[i]
		}

		// produce tombstone for key:
		log.Printf("%s: writing tombstone for key='%s'...\n", prefix, message.Key)
		partition, offset, err = prod.SendMessage(&sarama.ProducerMessage{
			Topic:     topic,
			Key:       sarama.ByteEncoder(message.Key),
			Value:     nil,
			Headers:   outputHeaders,
			Metadata:  nil,
			Partition: message.Partition,
		})
		if err != nil {
			log.Fatalln(err)
			return
		}
		if partition != message.Partition {
			log.Fatalf("%s: BUG: wrote to wrong partition! wrote to %d but should have written to %d\n", prefix, partition, message.Partition)
			return
		}

		log.Printf("%s: wrote tombstone for key='%s' at offset %7d\n", prefix, message.Key, offset)

	checkOffset:
		if message.Offset >= topicPartitionOffsetStop {
			log.Printf("%s: stopping because reached stop offset %7d\n", prefix, topicPartitionOffsetStop)
			break
		}
	}
}
