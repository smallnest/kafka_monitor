package main

import (
	"bytes"
	"sort"
	"strconv"
	"strings"
	"time"

	"flag"

	"fmt"

	"os"

	"github.com/Shopify/sarama"
	"github.com/fatih/color"
	"github.com/olekukonko/tablewriter"
	"github.com/samuel/go-zookeeper/zk"
)

var (
	zkAddr            = flag.String("zkAddr", "", "zookeeper address")
	basePath          = flag.String("basePath", "/kafka", "kafka base path in zookeeper")
	brokers           = flag.String("brokers", "localhost:9092", "brokers' address")
	topic             = flag.String("topic", "trigger", "topic name")
	group             = flag.String("group", "default", "consumer group name")
	version           = flag.String("version", "0.10.0.1", "kafka version. min version is 0.8.2.0")
	lagThreshold      = flag.Int("lagThreshold", 1000, "alarm lag threshold for partition")
	totalLagThreshold = flag.Int("totalLagThreshold", 5000, "alarm total lag threshold for topic")
	interval          = flag.Duration("duration", time.Minute, "check interval time")
	informEmail       = flag.String("email", "xxx@xxxxxx.com", "inform user email")
	smtpHost          = flag.String("smtp.host", "smtp.sina.com", "smtp host for sending alarms")
	smtpPort          = flag.Int("smtp.port", 25, "smtp port for sending alarms")
	smtpUser          = flag.String("smtp.user", "kafka_monitor", "smtp user for sending alarms")
	smtpPassword      = flag.String("smtp.password", "xxxxxx", "smtp user password for sending alarms")
)

var (
	maybeProblem       = false
	lastTriggeredTime  time.Time
	mergeAlertDuration = 10 * time.Minute
	kafkaVersions      = kafkaVersion()
)

func kafkaVersion() map[string]sarama.KafkaVersion {
	m := make(map[string]sarama.KafkaVersion)
	m["0.8.2.0"] = sarama.V0_8_2_0
	m["0.8.2.1"] = sarama.V0_8_2_1
	m["0.8.2.2"] = sarama.V0_8_2_2
	m["0.9.0.0"] = sarama.V0_9_0_0
	m["0.9.0.1"] = sarama.V0_9_0_1
	m["0.10.0.0"] = sarama.V0_10_0_0
	m["0.10.0.1"] = sarama.V0_10_0_1
	m["0.10.1.0"] = sarama.V0_10_1_0
	return m
}

func main() {
	flag.Parse()

	for {
		check()
	}
}

func check() {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("check error: %v", r)
			alert(*informEmail, *brokers, *topic, []byte(fmt.Sprintf("%v", r)), *smtpHost, *smtpPort, *smtpUser, *smtpPassword)
		}
	}()

	kafkaBrokers := strings.Split(*brokers, ",")
	v := kafkaVersions[*version]
	client := NewSaramaClient(kafkaBrokers, v)

	var buf bytes.Buffer

	ticker := time.NewTicker(*interval)
	for range ticker.C {
		buf.Reset()
		partitions, err := client.Partitions(*topic)
		if err != nil {
			fmt.Printf("failed to get partitions for topic=%s, err=%v\n", *topic, err)
			panic(err)
		}

		writablePartitions, err := client.WritablePartitions(*topic)
		if err != nil {
			fmt.Printf("failed to get writable partitions for topic=%s, err=%v\n", *topic, err)
			panic(err)
		}

		if len(partitions) != len(writablePartitions) {
			buf.WriteString("some partitions are not writable\n")
			buf.WriteString(fmt.Sprintf("all partitions: %v\n", partitions))
			buf.WriteString(fmt.Sprintf("writable partitions: %v\n", writablePartitions))
			//TODO print unwritable partitions
			maybeProblem = true
		}

		fmt.Printf("Brokers: %s\nVersion: %s\n", color.GreenString(*brokers), color.GreenString(*version))
		fmt.Printf("Topic: %s, Group: %s, Partitions: %s\n\n",
			color.GreenString(*topic), color.GreenString(*group), color.GreenString(strconv.Itoa(len(partitions))))

		//partition info
		var c *zk.Conn
		if *zkAddr != "" {
			c, _, err = zk.Connect(strings.Split(*zkAddr, ","), 5*time.Second)
			if err != nil {
				panic(err)
			}
		}

		infos := GetPartitionInfo(client, *topic, partitions, c, *basePath)
		if len(infos) > 0 {
			table := tablewriter.NewWriter(&buf)
			table.SetHeader([]string{"partition", "leader address", "leader", "replicas", "isr"})
			for _, info := range infos {
				replicas := fmt.Sprintf("%v", info.Replicas)
				replicas = replicas[1 : len(replicas)-1]
				replicas = compareString(replicas)
				isr := compareString(info.Isr)

				table.Append([]string{strconv.Itoa(int(info.Partition)), info.LeaderAddress, strconv.Itoa(int(info.Leader)), "[" + replicas + "]", "[" + isr + "]"})

				if replicas != isr {
					maybeProblem = true
				}
			}

			table.SetAlignment(tablewriter.ALIGN_LEFT)
			table.Render()

			buf.WriteString("\n\n")
		}
		//lag
		if offsets, err := FetchOffsets(client, *topic, *group); err == nil {

			table := tablewriter.NewWriter(&buf)
			table.SetHeader([]string{"partition", "end of log", "group offset", "lag"})

			var totalLag int64
			for _, info := range offsets {
				lag := info.PartitionOffset - info.GroupOffset
				if int(lag) > *lagThreshold {
					maybeProblem = true
				}

				totalLag += lag
				table.Append([]string{strconv.Itoa(int(info.Partition)), strconv.Itoa(int(info.PartitionOffset)),
					strconv.Itoa(int(info.GroupOffset)), convertLag(lag, *lagThreshold)})
			}

			if int(totalLag) > *totalLagThreshold {
				maybeProblem = true
			}

			table.SetFooter([]string{"", "", "Total", convertLag(totalLag, *totalLagThreshold)})
			table.SetAlignment(tablewriter.ALIGN_LEFT)
			table.SetFooterAlignment(tablewriter.ALIGN_LEFT)
			table.Render()
		}

		//output tables to stdout
		bytes := buf.Bytes()
		os.Stdout.Write(bytes)

		if maybeProblem && time.Since(lastTriggeredTime) > mergeAlertDuration {
			lastTriggeredTime = time.Now()
			alert(*informEmail, *brokers, *topic, bytes, *smtpHost, *smtpPort, *smtpUser, *smtpPassword)
		}

		maybeProblem = false
	}
}

func compareString(s string) string {
	s1 := strings.Split(s, " ")
	sort.Sort(sort.StringSlice(s1))

	return strings.Join(s1, " ")
}
func convertLag(lag int64, threshold int) string {
	lagStr := strconv.Itoa(int(lag))
	if int(lag) > threshold {
		lagStr = color.RedString(lagStr)
	}

	return lagStr
}

func NewSaramaClient(brokers []string, version sarama.KafkaVersion) sarama.Client {
	config := sarama.NewConfig()
	config.Version = version
	config.Consumer.Return.Errors = true
	config.Metadata.RefreshFrequency = 1 * time.Minute
	config.Metadata.Retry.Max = 10
	config.Net.MaxOpenRequests = 10
	config.Net.DialTimeout = 10 * time.Second
	config.Net.ReadTimeout = 10 * time.Second
	config.Net.WriteTimeout = 10 * time.Second

	client, err := sarama.NewClient(brokers, config)

	if err != nil {
		panic("Failed to start client: " + err.Error())
	}

	return client
}
