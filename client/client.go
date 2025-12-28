package main

import (
	"context"
	"log"
	"time"

	pb "github.com/zukigit/learn-gRPC/greet"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	address = "localhost:50051"
)

func main() {
	// Set up a connection to the server
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	c := pb.NewGreeterClient(conn)

	// Contact the server and print out its response
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Unary RPC call
	name := "World"
	r, err := c.SayHello(ctx, &pb.HelloRequest{Name: name})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	log.Printf("Greeting: %s", r.GetMessage())

	// Streaming RPC call
	log.Println("Starting streaming RPC...")
	streamCtx, streamCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer streamCancel()

	stream, err := c.SayHelloStream(streamCtx, &pb.HelloRequest{Name: "Streaming Client"})
	if err != nil {
		log.Fatalf("could not start stream: %v", err)
	}

	for {
		response, err := stream.Recv()
		if err != nil {
			log.Printf("Stream ended: %v", err)
			break
		}
		log.Printf("Stream response: %s", response.GetMessage())
	}
}
