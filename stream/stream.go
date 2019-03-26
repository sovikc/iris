package stream

import (
	"errors"

	uuid "github.com/gofrs/uuid"
)

const (
	//READY represents stream is initialized or being used and not full
	// It changes to READY as soon as 1 event has been written
	READY = 0
	//ACTIVE represents being used and not full
	ACTIVE = 1
	// FULL represents the stream is full
	FULL = 2
	// LOCKED represents the stream is full and is being written to disk
	LOCKED = 3
	//OPEN   = 4

	BUFFER1 = 0
	BUFFER2 = 1
)

type Topic struct {
	ID               string
	Name             string
	ActivePartitions map[string]*Partition
	numPartitions    []int
}

type Partition struct {
	ID              string
	Name            string
	PartitionHandle *Partition
	Topic           string
	Path            string
	Streams         []*Stream
	Status          int
	MaxBuffers      int
}

func (p *Partition) GetStreams() []*Stream {
	return p.Streams
}

type Stream struct {
	ID       int
	Name     string
	Status   int
	Lock     int
	Size     int
	Messages []Message
	MaxSize  int
}

type Message struct {
	Topic     string
	Partition string
	User      string
	Delta     string
	Operation string
	Timestamp string
}

//NewTopic is used to create the topic across the cluster.
func NewTopic(name string) (*Topic, error) {
	uid, err := uuid.NewV4()
	if err != nil {
		return nil, err
	}

	t := &Topic{
		ID:               uid.String(),
		Name:             name,
		ActivePartitions: make(map[string]*Partition),
	}

	return t, nil
}

//NewPartition is used to create a partition for a Topic
func NewPartition(t *Topic, name string) (*Partition, error) {

	partition, exists := t.ActivePartitions[name]
	if exists {
		return nil, errors.New("Partition exists or in an active mode")
	}

	pID, _ := uuid.NewV4()
	partition = &Partition{
		ID:              pID.String(),
		Name:            name,
		PartitionHandle: partition,
		Topic:           t.Name,
		MaxBuffers:      2,
		Streams:         make([]*Stream, 0),
	}

	t.ActivePartitions[name] = partition

	for i := 0; i < partition.MaxBuffers; i++ {
		stream := &Stream{
			ID:       i + 1,
			Status:   READY,
			Size:     0,
			Messages: make([]Message, 0, 5),
			MaxSize:  5,
		}
		partition.Streams = append(partition.Streams, stream)
	}

	return partition, nil
}

//GetStream returns the stream with status ACTIVE or READY
func GetStream(p *Partition) (*Stream, error) {
	if p == nil {
		return nil, errors.New("invalid partition")
	}
	stream1, stream2 := p.Streams[0], p.Streams[1]

	switch {
	case stream1.Status == LOCKED && stream2.Status == LOCKED:
		return nil, errors.New("buffers not available for the new message")
	case stream1.Status == READY && stream2.Status == READY:
		return stream1, nil
	case stream1.Status == FULL && stream2.Status == READY:
		return stream2, nil
	case stream1.Status == READY && stream2.Status == FULL:
		return stream1, nil
	case stream1.Status == ACTIVE:
		return stream1, nil
	case stream2.Status == ACTIVE:
		return stream2, nil
	default:
		return nil, nil
	}
}

func CloseActiveSteam(streams []*Stream) error {
	if streams == nil {
		return errors.New("no streams argument")
	}

	for _, stream := range streams {
		if stream.Status == ACTIVE {
			stream.Status = FULL
		}
	}
	return nil
}

//Write
func Write(stream *Stream, m Message, activeBuffState chan int) {
	stream.Messages = append(stream.Messages, m)
	if len(stream.Messages) < 5 {
		stream.Status = ACTIVE
		activeBuffState <- ACTIVE
		return
	}

	stream.Status = FULL
	activeBuffState <- FULL
}
