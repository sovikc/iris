package main

import (
	"context"
	"io"
	"log"
	"net"

	"github.com/sovikc/iris/event/eventpb"
	"github.com/sovikc/iris/stream"
	"google.golang.org/grpc"
)

var path string
var topics map[string]*stream.Topic
var lastActivePartition *stream.Partition

func init() {
	topics = make(map[string]*stream.Topic)
	topicNames := []string{"Document", "Wireframe", "Image"}
	for _, name := range topicNames {
		tp, err := stream.NewTopic(name)
		if err != nil {
			log.Fatalf("Error %v ", err)
		}

		err = tp.CreateTopicDirectory()
		if err != nil {
			log.Fatalf("Error %v ", err)
		}

		topics[name] = tp
	}
}

type server struct{}

func (s *server) Create(ctx context.Context,
	req *eventpb.CreateEventRequest) (*eventpb.CreateEventResponse, error) {

	name := req.CreateEvent.GetName()
	topicKey := req.CreateEvent.GetTopic()

	topic := topics[topicKey]
	partition, err := stream.NewPartition(topic, name)
	if err != nil {
		return nil, err
	}

	err = partition.CreatePartitionFile()
	if err != nil {
		return nil, err
	}
	//activePartitions[name] = partition
	result := " Document Created " + partition.ID + " : " + partition.Name
	return &eventpb.CreateEventResponse{
		Result: result,
	}, nil
}

func (s *server) Edit(evtStream eventpb.EventService_EditServer) error {
	activeBuffState := make(chan int)

	for {
		req, err := evtStream.Recv()
		if err == io.EOF {
			//bufferStream.Status = FULL
			// we have finished the client stream
			stream.CloseActiveSteam(lastActivePartition.GetStreams())
			return evtStream.SendAndClose(&eventpb.EditEventResponse{
				Result: "stream closed",
			})
		}
		if err != nil {
			log.Fatalf("Error while reading client stream %v", err)
		}

		message := getMessage(req)

		topic := topics[message.Topic]
		prt := topic.ActivePartitions[message.Partition]
		lastActivePartition = prt
		bufferStream, err := stream.GetStream(prt)
		if err != nil {
			return err
		}
		go stream.Write(bufferStream, message, activeBuffState)
		bufState := <-activeBuffState
		logErr := make(chan error)
		go stream.WriteToLog(bufferStream, bufState, message.Topic, message.Partition, logErr)
		err = <-logErr
		if err != nil {
			log.Fatalf("Error while saving stream %v", err)
		}

	}
}

func getMessage(req *eventpb.EditEventRequest) stream.Message {
	name := req.EditEvent.GetName()
	topicKey := req.EditEvent.GetTopic()
	user := req.EditEvent.GetUser()
	delta := req.EditEvent.GetDelta()
	operation := req.EditEvent.GetOperation()
	//timeStamp := req.EditEvent.GetTimestamp()

	return stream.Message{
		Topic:     topicKey,
		Partition: name,
		User:      user,
		Delta:     delta,
		Operation: operation,
		//Timestamp: timeStamp,
	}
}

func main() {
	lis, err := net.Listen("tcp", "0.0.0.0:50051")
	if err != nil {
		log.Fatalf("failed to listen %v ", err)
	}

	s := grpc.NewServer()
	eventpb.RegisterEventServiceServer(s, &server{})

	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve %v", err)
	}
}
