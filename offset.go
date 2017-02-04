package main

import (
	"fmt"

	"github.com/Shopify/sarama"
)

type OffsetInfo struct {
	Topic           string
	Partition       int32
	PartitionOffset int64
	GroupOffset     int64
}

func FetchOffsets(client sarama.Client, topic string, group string) ([]*OffsetInfo, error) {
	var infos []*OffsetInfo

	partitions, err := client.Partitions(topic)
	if err != nil {
		fmt.Printf("failed to get partitions for topic=%s, err=%v\n", topic, err)
		return nil, err
	}

	offsetManager, err := sarama.NewOffsetManagerFromClient(group, client)
	if err != nil {
		fmt.Printf("failed to create offset manager for topic=%s, group=%s err=%v\n", topic, group, err)
		return nil, err
	}

	for _, partition := range partitions {
		partitionOffsetManager, err := offsetManager.ManagePartition(topic, partition)
		if err != nil {
			fmt.Printf("failed to get partition manager for topic=%s, partition=%d, err=%v\n", topic, partition, err)
			continue
		}
		cgOffset, _ := partitionOffsetManager.NextOffset()

		pOffset, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
		if err != nil {
			fmt.Printf("failed to get partition offset for topic=%s, partition=%d, err=%v\n", topic, partition, err)
			continue
		}
		info := &OffsetInfo{
			Topic:           topic,
			Partition:       partition,
			PartitionOffset: pOffset,
			GroupOffset:     cgOffset,
		}

		infos = append(infos, info)
	}

	return infos, nil
}
