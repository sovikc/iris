package stream_test

import (
	"testing"

	uuid "github.com/gofrs/uuid"
	"github.com/sovikc/iris/stream"
	"github.com/stretchr/testify/require"
)

func TestNewTopic(t *testing.T) {
	var topicName string = "testTopic"
	topic, err := stream.NewTopic(topicName)
	require.NoError(t, err)

	require.Equal(t, topicName, topic.Name)

}

func TestNewPartition(t *testing.T) {
	var topicName string = "testTopic"
	var partitionName string = "testPartition"

	uid, err := uuid.NewV4()
	require.NoError(t, err)
	topic := &stream.Topic{
		ID:               uid.String(),
		Name:             topicName,
		ActivePartitions: make(map[string]*stream.Partition),
	}

	partition, err := stream.NewPartition(topic, partitionName)
	require.NoError(t, err)
	require.Equal(t, partitionName, partition.Name)
	require.Equal(t, 2, len(partition.Streams))
}
