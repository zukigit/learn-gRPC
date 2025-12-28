package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	pb "github.com/zukigit/learn-gRPC/greet"

	"google.golang.org/grpc"
)

const (
	port = ":50051"
)

type server struct {
	pb.UnimplementedGreeterServer
}

// SayHello implements the Greeter service
func (s *server) SayHello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	log.Printf("Received: %v", in.GetName())
	return &pb.HelloReply{Message: "Hello " + in.GetName()}, nil
}

// SayHelloStream implements the streaming RPC
func (s *server) SayHelloStream(in *pb.HelloRequest, stream pb.Greeter_SayHelloStreamServer) error {
	log.Printf("Streaming response for: %v", in.GetName())

	for i := 0; i < 5; i++ {
		message := &pb.HelloReply{
			Message: fmt.Sprintf("Hello %s, message #%d", in.GetName(), i+1),
		}
		if err := stream.Send(message); err != nil {
			return err
		}
		time.Sleep(1 * time.Second)
	}
	return nil
}

func main() {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterGreeterServer(s, &server{})

	log.Printf("Server listening at %v", lis.Addr())

	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
