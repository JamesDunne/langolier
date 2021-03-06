package main

import (
	"bytes"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/karrick/tparse"
	flag "github.com/spf13/pflag"
	"langolier/env"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	processTmpDir string
)

func main() {
	var err error

	var (
		topic string

		partitionsStr string
		partitions    []int32

		timeStartStr, timeStopStr string
		timeStart, timeStop       time.Time

		headerName      string
		headerValuesStr string
		headerValues    []string

		logTombstones  bool
		logBadMessages bool
		logPosMatch    bool
		logNegMatch    bool
	)

	fs := flag.FlagSet{
		Usage:                nil,
		SortFlags:            false,
		ParseErrorsWhitelist: flag.ParseErrorsWhitelist{},
	}

	fs.StringVar(&topic, "topic", "", "kafka topic name; REQUIRED")

	fs.StringVar(&headerName, "header", "tenantId", "header name to match on")
	fs.StringVar(&headerValuesStr, "values", "", "header values to match on (where value in [values]; comma-delimited; REQUIRED)")

	fs.StringVar(&partitionsStr, "partitions", "all", "partitions to process; comma-delimited")

	fs.StringVar(&timeStartStr, "start", "0", "start time (inclusive)")
	fs.StringVar(&timeStopStr, "stop", "now", "stop time (inclusive)")

	// "-log-ts=false" to disable these
	fs.BoolVar(&logTombstones, "log-ts", true, "log tombstone messages consumed")
	fs.BoolVar(&logBadMessages, "log-bad", false, "log bad messages which have no headers")
	fs.BoolVar(&logPosMatch, "log-pos", true, "log positive matched messages")
	fs.BoolVar(&logNegMatch, "log-neg", false, "log negative matched messages")

	err = fs.Parse(os.Args[1:])
	if err != nil {
		fmt.Println(err)
		return
	}
	valid := true

	// validate required arguments:
	if headerValuesStr == "" {
		fmt.Println("required --values argument")
		valid = false
	}
	if topic == "" {
		fmt.Println("required --topic argument")
		valid = false
	}
	if !valid {
		fs.PrintDefaults()
		return
	}

	fs.PrintDefaults()

	timeStart, err = tparse.ParseNow(time.RFC3339, timeStartStr)
	if err != nil {
		fmt.Printf("error parsing '--start' argument '%s': %v\n", timeStartStr, err)
		timeStart = time.Unix(0, 0)
		err = nil
	}
	fmt.Printf("parsed --start '%s' as '%s'\n", timeStartStr, timeStart)

	timeStop, err = tparse.ParseNow(time.RFC3339, timeStopStr)
	if err != nil {
		fmt.Printf("error parsing '--stop' argument '%s': %v\n", timeStopStr, err)
		timeStop = time.Now()
		err = nil
	}
	fmt.Printf("parsed --stop '%s' as '%s'\n", timeStopStr, timeStop)

	headerValues = strings.Split(headerValuesStr, ",")

	headerNameBytes := []byte(headerName)
	headerValuesBytes := make([][]byte, len(headerValues))
	for i := range headerValues {
		headerValuesBytes[i] = []byte(headerValues[i])
	}

	// set up logging:
	log.SetOutput(os.Stdout)
	log.SetFlags(log.LUTC | log.Lmicroseconds | log.Ltime)

	sarama.Logger = log.New(os.Stderr, "kafka: ", log.LUTC|log.Lmicroseconds|log.Ltime|log.Lmsgprefix)

	processTmpDir = env.GetOrSupply("KAFKA_TMP", func() string {
		return os.TempDir()
	})
	processTmpDir = filepath.Join(processTmpDir, strconv.Itoa(os.Getpid()))
	err = os.MkdirAll(processTmpDir, 0700)
	if err != nil {
		log.Fatalln(err)
		return
	}

	// process env vars for kafka config:
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

	// create a kafka client:
	var client sarama.Client
	client, err = sarama.NewClient(addrs, conf)
	if err != nil {
		return
	}

	defer func() {
		err := client.Close()
		if err != nil {
			log.Println(err)
		}
	}()

	if partitionsStr == "all" {
		partitions, err = client.Partitions(topic)
	} else {
		pStrs := strings.Split(partitionsStr, ",")
		partitions = make([]int32, len(pStrs))
		for i, str := range pStrs {
			val, err := strconv.Atoi(str)
			if err != nil {
				val = 0
			}
			partitions[i] = int32(val)
		}
		// TODO: filter out bad partition numbers
	}

	var cons sarama.Consumer
	cons, err = sarama.NewConsumerFromClient(client)
	if err != nil {
		return
	}

	var prod sarama.SyncProducer
	prod, err = sarama.NewSyncProducerFromClient(client)
	if err != nil {
		return
	}

	closing := make(chan struct{})
	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGTERM, os.Interrupt)
		<-signals

		log.Println("Initiating shutdown of consumer...")
		close(closing)
	}()

	err = tombstonePartitions(
		ClientContext{
			Client:  client,
			Cons:    cons,
			Prod:    prod,
			Closing: closing,
		},
		TombstoneConfig{
			Topic:             topic,
			Partitions:        partitions,
			TimeStart:         timeStart,
			TimeStop:          timeStop,
			HeaderNameBytes:   headerNameBytes,
			HeaderValuesBytes: headerValuesBytes,

			LogTombstones:  logTombstones,
			LogBadMessages: logBadMessages,
			LogPosMatch:    logPosMatch,
			LogNegMatch:    logNegMatch,
		},
	)
	if err != nil {
		log.Println(err)
	}

	log.Println("Initiating shutdown of producer...")
	err = prod.Close()
	if err != nil {
		log.Printf("Error shutting down producer: %v\n", err)
	}
}

type ClientContext struct {
	Client  sarama.Client
	Cons    sarama.Consumer
	Prod    sarama.SyncProducer
	Closing chan struct{}
}

type TombstoneConfig struct {
	Topic string

	Partitions []int32

	TimeStart time.Time
	TimeStop  time.Time

	HeaderNameBytes   []byte
	HeaderValuesBytes [][]byte

	LogTombstones  bool
	LogBadMessages bool
	LogPosMatch    bool
	LogNegMatch    bool
}

func tombstonePartitions(
	ctx ClientContext,
	tombstoneConfig TombstoneConfig,
) (err error) {
	client := ctx.Client
	cons := ctx.Cons
	prod := ctx.Prod
	closing := ctx.Closing

	// refresh topic metadata but no big deal if not successful:
	topic := tombstoneConfig.Topic
	err = client.RefreshMetadata(topic)
	if err != nil {
		log.Println(err)
		err = nil
	}

	// query offset ranges per each partition:
	offsetStarts := make(map[int32]int64, len(tombstoneConfig.Partitions))
	offsetStops := make(map[int32]int64, len(tombstoneConfig.Partitions))
	for _, partition := range tombstoneConfig.Partitions {
		// query partition for offsets based on times (in milliseconds):
		var offsetStart, offsetStop int64
		offsetStart, err = client.GetOffset(topic, partition, tombstoneConfig.TimeStart.UnixNano()/1_000_000)
		if err != nil {
			return
		}
		if offsetStart == -1 {
			offsetStart, err = client.GetOffset(topic, partition, sarama.OffsetOldest)
			err = nil
		}
		log.Printf("queried  partition %3d offset for start time %s = %7d offset\n", partition, tombstoneConfig.TimeStart, offsetStart)

		offsetStop, err = client.GetOffset(topic, partition, tombstoneConfig.TimeStop.UnixNano()/1_000_000)
		if err != nil {
			return
		}
		if offsetStop == -1 {
			offsetStop, err = client.GetOffset(topic, partition, sarama.OffsetNewest)
			err = nil
		}
		log.Printf("queried  partition %3d offset for stop  time %s = %7d offset\n", partition, tombstoneConfig.TimeStop, offsetStop)

		offsetStarts[partition], offsetStops[partition] = offsetStart, offsetStop
	}

	// consume each partition individually:
	wg := sync.WaitGroup{}
	wg.Add(len(tombstoneConfig.Partitions))
	for _, partition := range tombstoneConfig.Partitions {
		go func(partition int32) {
			offsetStart := offsetStarts[partition]
			offsetStop := offsetStops[partition]

			// consume a partition from start to end:
			log.Printf(
				"[%s][%3d] creating partition consumer from offset %7d to %7d (inclusive)",
				topic,
				partition,
				offsetStart,
				offsetStop,
			)
			var pc sarama.PartitionConsumer
			pc, err = cons.ConsumePartition(topic, partition, offsetStart)
			if err != nil {
				wg.Done()
				return
			}
			log.Printf(
				"[%s][%3d] created partition consumer from offset %7d to %7d (inclusive)",
				topic,
				partition,
				offsetStart,
				offsetStop,
			)

			go func(pc sarama.PartitionConsumer) {
				<-closing

				log.Printf(
					"[%s][%3d] stopping partition consumer",
					topic,
					partition,
				)
				pc.AsyncClose()
			}(pc)

			go func(pc sarama.PartitionConsumer) {
				var err error

				defer wg.Done()

				log.Printf(
					"[%s][%3d] consuming partition from offset %7d to %7d (inclusive)",
					topic,
					partition,
					offsetStart,
					offsetStop,
				)

			messageLoop:
				for message := range pc.Messages() {
					// quick check if closing down:
					select {
					case <-closing:
						break messageLoop
					default:
						break
					}

					var match bool
					var mpartition int32
					var moffset int64
					var outputHeaders []sarama.RecordHeader

					mpartition = message.Partition
					moffset = message.Offset

					prefix := fmt.Sprintf("[%s][%3d][%7d/%7d]", topic, mpartition, moffset, offsetStop)

					// stop is inclusive, so only check if we've consumed an offset greater than it:
					if moffset > offsetStop {
						log.Printf("%s: stopping because consuming next message would exceed stop offset %7d\n", prefix, offsetStop)
						break messageLoop
					}

					// skip existing tombstones:
					if message.Value == nil {
						if tombstoneConfig.LogTombstones {
							log.Printf("%s: skipping message for key='%s' because tombstone...\n", prefix, message.Key)
						}
						goto checkOffset
					}
					if len(message.Value) == 0 {
						if tombstoneConfig.LogTombstones {
							log.Printf("%s: skipping message for key='%s' because tombstone...\n", prefix, message.Key)
						}
						goto checkOffset
					}
					if len(message.Headers) == 0 {
						if tombstoneConfig.LogBadMessages {
							log.Printf("%s: skipping message for key='%s' because no headers found...\n", prefix, message.Key)
						}
						goto checkOffset
					}

					// check header values:
					match = false
				findMatch:
					for _, h := range message.Headers {
						// check header key:
						if bytes.Compare(h.Key, tombstoneConfig.HeaderNameBytes) != 0 {
							continue
						}

						// check header value:
						for _, v := range tombstoneConfig.HeaderValuesBytes {
							if bytes.Compare(h.Value, v) == 0 {
								match = true
								if tombstoneConfig.LogPosMatch {
									log.Printf("%s: matched key='%s' on header %s='%s'\n", prefix, message.Key, h.Key, h.Value)
								}
								break findMatch
							}
						}
					}

					if !match {
						if tombstoneConfig.LogNegMatch {
							log.Printf("%s: skipping message for key='%s' because no header matches...\n", prefix, message.Key)
						}
						goto checkOffset
					}

					// copy headers:
					outputHeaders = make([]sarama.RecordHeader, len(message.Headers))
					for i := range message.Headers {
						outputHeaders[i] = *message.Headers[i]
					}

					// produce tombstone for key:
					log.Printf("%s: writing tombstone for key='%s'...\n", prefix, message.Key)
					mpartition, moffset, err = prod.SendMessage(&sarama.ProducerMessage{
						Topic:     topic,
						Key:       sarama.ByteEncoder(message.Key),
						Value:     nil,
						Headers:   outputHeaders,
						Metadata:  nil,
						Partition: message.Partition,
					})
					if err != nil {
						log.Printf("%s: %v\n", prefix, err)
						return
					}
					if mpartition != message.Partition {
						err = fmt.Errorf("%s: BUG: wrote to wrong partition! wrote to %d but should have written to %d\n", prefix, mpartition, message.Partition)
						log.Println(err)
						return
					}

					log.Printf("%s: wrote tombstone for key='%s' at offset %7d\n", prefix, message.Key, moffset)

				checkOffset:
					if message.Offset >= offsetStop {
						log.Printf("%s: stopping because reached stop offset %7d\n", prefix, offsetStop)
						break messageLoop
					}
				}
			}(pc)
		}(partition)
	}
	wg.Wait()

	err = nil
	return
}
