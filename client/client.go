package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/sovikc/iris/event/eventpb"
	"google.golang.org/grpc"
)

func main() {
	cc, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Could not connect %v", err)
	}
	defer cc.Close()
	c := eventpb.NewEventServiceClient(cc)
	createWireframe(c)
	doClientStreaming(c, "Wireframe")
}

func createWireframe(c eventpb.EventServiceClient) {
	req := &eventpb.CreateEventRequest{
		CreateEvent: &eventpb.CreateEvent{
			Topic:     "Wireframe",
			Name:      "CollectingMetrics",
			User:      "Tics",
			Timestamp: "2019-10-20 00:00:00.000 ",
		},
	}
	res, err := c.Create(context.Background(), req)
	if err != nil {
		log.Fatalf("error while calling Create Document: %v", err)
	}
	fmt.Printf("Response from Event Request %v ", res.Result)
	fmt.Println()
}

func doClientStreaming(c eventpb.EventServiceClient, topicName string) {
	requests := []*eventpb.EditEventRequest{
		&eventpb.EditEventRequest{
			EditEvent: &eventpb.EditEvent{
				Name:      "CollectingMetrics",
				Topic:     topicName,
				User:      "tixu",
				Operation: "add",
				Delta:     "Th",
			},
		},
		&eventpb.EditEventRequest{
			EditEvent: &eventpb.EditEvent{
				Name:      "CollectingMetrics",
				Topic:     topicName,
				User:      "tixu",
				Operation: "add",
				Delta:     "e",
			},
		},
		&eventpb.EditEventRequest{
			EditEvent: &eventpb.EditEvent{
				Name:      "CollectingMetrics",
				Topic:     topicName,
				User:      "tixu",
				Operation: "add",
				Delta:     " ",
			},
		},
		&eventpb.EditEventRequest{
			EditEvent: &eventpb.EditEvent{
				Name:      "CollectingMetrics",
				Topic:     topicName,
				User:      "tixu",
				Operation: "add",
				Delta:     "qu",
			},
		},
		&eventpb.EditEventRequest{
			EditEvent: &eventpb.EditEvent{
				Name:      "CollectingMetrics",
				Topic:     topicName,
				User:      "tixu",
				Operation: "add",
				Delta:     "i",
			},
		},
		&eventpb.EditEventRequest{
			EditEvent: &eventpb.EditEvent{
				Name:      "CollectingMetrics",
				Topic:     topicName,
				User:      "tixu",
				Operation: "add",
				Delta:     "ck",
			},
		},
		&eventpb.EditEventRequest{
			EditEvent: &eventpb.EditEvent{
				Name:      "CollectingMetrics",
				Topic:     topicName,
				User:      "tixu",
				Operation: "add",
				Delta:     "br",
			},
		},
		&eventpb.EditEventRequest{
			EditEvent: &eventpb.EditEvent{
				Name:      "CollectingMetrics",
				Topic:     topicName,
				User:      "tixu",
				Operation: "add",
				Delta:     "o",
			},
		},
		&eventpb.EditEventRequest{
			EditEvent: &eventpb.EditEvent{
				Name:      "CollectingMetrics",
				Topic:     topicName,
				User:      "tixu",
				Operation: "add",
				Delta:     "wn",
			},
		},
		&eventpb.EditEventRequest{
			EditEvent: &eventpb.EditEvent{
				Name:      "CollectingMetrics",
				Topic:     topicName,
				User:      "tixu",
				Operation: "add",
				Delta:     "fo",
			},
		},
		&eventpb.EditEventRequest{
			EditEvent: &eventpb.EditEvent{
				Name:      "CollectingMetrics",
				Topic:     topicName,
				User:      "tixu",
				Operation: "add",
				Delta:     "x",
			},
		},
	}

	stream, err := c.Edit(context.Background())
	if err != nil {
		log.Fatalf("Error while calling EditEvent: %v", err)
	}

	for _, req := range requests {
		log.Println(req)
		stream.Send(req)
		time.Sleep(100 * time.Millisecond)
	}

	res, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("error while receiving response from EditEvent: %v", err)
	}
	fmt.Printf("EditEvent Response: %v\n", res)
}
