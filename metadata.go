package main

import (
	"fmt"

	"strings"

	"github.com/Shopify/sarama"
	"github.com/samuel/go-zookeeper/zk"
)

type PartitionInfo struct {
	Topic         string
	Partition     int32
	Leader        int32
	LeaderAddress string
	Replicas      []int32
	Isr           string
}

func GetPartitionInfo(client sarama.Client, topic string, partitions []int32, c *zk.Conn, basePath string) []*PartitionInfo {
	var infos []*PartitionInfo
	for _, partition := range partitions {
		replicas, err := client.Replicas(topic, partition)
		if err != nil {
			fmt.Printf("failed to get replicas for topic=%s, partition=%d\n", topic, partition)
			leader, _ := client.Leader(topic, partition)
			infos = append(infos, &PartitionInfo{Topic: topic, Partition: partition, Leader: leader.ID(), Replicas: nil})
			continue
		}

		leader, _ := client.Leader(topic, partition)

		info := &PartitionInfo{
			Topic:         topic,
			Partition:     partition,
			Leader:        leader.ID(),
			LeaderAddress: leader.Addr(),
			Replicas:      replicas,
			Isr:           getIsr(c, basePath, topic, partition)}
		infos = append(infos, info)
	}

	return infos
}

func getIsr(conn *zk.Conn, basePath string, topic string, partition int32) string {
	if conn == nil {
		return ""
	}
	path := fmt.Sprintf("%s/brokers/topics/%s/partitions/%d/state", basePath, topic, partition)
	bytes, _, err := conn.Get(path)
	if err != nil {
		fmt.Printf("failed to get ISR for topic=%s, partition=%d becaue of %v\n", topic, partition, err)
		return ""
	}

	//{"controller_epoch":6,"leader":1,"version":1,"leader_epoch":23,"isr":[1,4,32]}
	isr := string(bytes)
	i := strings.Index(isr, `isr":[`)
	isr = isr[i+6:]
	i = strings.Index(isr, "]")
	isr = isr[:i]
	return strings.Replace(isr, ",", " ", -1)
}
