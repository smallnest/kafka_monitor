package main

import "github.com/Shopify/sarama"
import "fmt"

type PartitionInfo struct {
	Topic         string
	Partition     int32
	Leader        int32
	LeaderAddress string
	Isr           []int32
}

func GetPartitionInfo(client sarama.Client, topic string, partitions []int32) []*PartitionInfo {
	var infos []*PartitionInfo
	for _, partition := range partitions {
		replicas, err := client.Replicas(topic, partition)
		if err != nil {
			fmt.Printf("failed to get replicas for topic=%s, partition=%d\n", topic, partition)
			leader, _ := client.Leader(topic, partition)
			infos = append(infos, &PartitionInfo{Topic: topic, Partition: partition, Leader: leader.ID(), Isr: nil})
			continue
		}

		leader, _ := client.Leader(topic, partition)

		info := &PartitionInfo{
			Topic:         topic,
			Partition:     partition,
			Leader:        leader.ID(),
			LeaderAddress: leader.Addr(),
			Isr:           replicas}
		infos = append(infos, info)
	}

	return infos
}
