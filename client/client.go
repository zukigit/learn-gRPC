package main

import (
	"context"
	"log"
	"time"

	pb "github.com/zukigit/learn-gRPC/greet"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

const (
	address = "localhost:50051"
)

type AuthClient struct {
	token   string
	expires int64
	client  pb.GreeterClient
	conn    *grpc.ClientConn
}

// clientAuthInterceptor is a client interceptor that adds an auth token to the context
func clientAuthInterceptor(token *string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		// Skip adding token for Login and Register methods
		if method != "/greet.Greeter/Login" &&
			method != "/greet.Greeter/Register" &&
			*token != "" {
			// Add the authorization token in Bearer format
			ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+*token)
		}
		// Call the original invoker function
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// streamAuthInterceptor for streaming calls
func streamAuthInterceptor(token *string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		// Add token for streaming methods
		if *token != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+*token)
		}
		return streamer(ctx, desc, cc, method, opts...)
	}
}

func NewAuthClient() (*AuthClient, error) {
	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}

	return &AuthClient{
		client: pb.NewGreeterClient(conn),
		conn:   conn,
	}, nil
}

func (ac *AuthClient) Close() {
	if ac.conn != nil {
		ac.conn.Close()
	}
}

func (ac *AuthClient) Login(username, password string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := ac.client.Login(ctx, &pb.LoginRequest{
		Username: username,
		Password: password,
	})
	if err != nil {
		return err
	}

	ac.token = resp.Token
	ac.expires = resp.ExpiresAt

	// Decode JWT to show info (for demo)
	log.Printf("Login successful!")
	log.Printf("Token: %s...", resp.Token[:20])
	log.Printf("Expires at: %v", time.Unix(resp.ExpiresAt, 0).Format("2006-01-02 15:04:05"))

	// Update client with auth interceptor
	ac.updateClientWithAuth()
	return nil
}

func (ac *AuthClient) Register(username, password string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := ac.client.Register(ctx, &pb.RegisterRequest{
		Username: username,
		Password: password,
	})
	if err != nil {
		return err
	}

	log.Printf("Registration successful: %s", resp.Message)
	return nil
}

func (ac *AuthClient) Logout() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Add auth token to context for logout
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+ac.token)

	resp, err := ac.client.Logout(ctx, &pb.LogoutRequest{})
	if err != nil {
		return err
	}

	log.Printf("Logout successful: %s", resp.Message)
	ac.token = "" // Clear token
	return nil
}

func (ac *AuthClient) GetProfile() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := ac.client.GetProfile(ctx, &pb.ProfileRequest{})
	if err != nil {
		return err
	}

	log.Printf("Profile Info:")
	log.Printf("  Username: %s", resp.Username)
	log.Printf("  Role: %s", resp.Role)
	log.Printf("  Message: %s", resp.Message)
	return nil
}

func (ac *AuthClient) updateClientWithAuth() {
	// Close old connection
	if ac.conn != nil {
		ac.conn.Close()
	}

	// Create new connection with auth interceptor
	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(clientAuthInterceptor(&ac.token)),
		grpc.WithStreamInterceptor(streamAuthInterceptor(&ac.token)),
	)
	if err != nil {
		log.Fatalf("Failed to create auth client: %v", err)
	}

	ac.client = pb.NewGreeterClient(conn)
	ac.conn = conn
}

func (ac *AuthClient) SayHello(name string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := ac.client.SayHello(ctx, &pb.HelloRequest{Name: name})
	if err != nil {
		return err
	}

	log.Printf("Greeting: %s", resp.GetMessage())
	return nil
}

func (ac *AuthClient) SayHelloStream(name string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := ac.client.SayHelloStream(ctx, &pb.HelloRequest{Name: name})
	if err != nil {
		return err
	}

	log.Println("Starting streaming RPC...")
	for {
		response, err := stream.Recv()
		if err != nil {
			log.Printf("Stream ended: %v", err)
			break
		}
		log.Printf("Stream response: %s", response.GetMessage())
	}
	return nil
}

func main() {
	// Create client without auth initially
	client, err := NewAuthClient()
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Test registration
	log.Println("\nTesting registration...")
	err = client.Register("newuser", "newpass123")
	if err != nil {
		log.Printf("Registration error (might be duplicate): %v", err)
	}

	// Try to call SayHello without login (should fail)
	log.Println("\nAttempting to call SayHello without login...")
	err = client.SayHello("World")
	if err != nil {
		log.Printf("Expected error without token: %v", err)
	}

	// Login to get JWT token
	log.Println("\nLogging in...")
	err = client.Login("admin", "admin123")
	if err != nil {
		log.Fatalf("Login failed: %v", err)
	}

	// Test protected profile endpoint
	log.Println("\nGetting user profile...")
	err = client.GetProfile()
	if err != nil {
		log.Printf("GetProfile failed: %v", err)
	}

	// Now call SayHello with valid token
	log.Println("\nCalling SayHello with valid token...")
	err = client.SayHello("World")
	if err != nil {
		log.Fatalf("SayHello failed: %v", err)
	}

	// Test streaming with valid token
	log.Println("\nTesting streaming with valid token...")
	err = client.SayHelloStream("Streaming Client")
	if err != nil {
		log.Fatalf("SayHelloStream failed: %v", err)
	}

	// Test logout
	log.Println("\nTesting logout...")
	err = client.Logout()
	if err != nil {
		log.Printf("Logout failed: %v", err)
	}

	// Try to call SayHello after logout (should fail)
	log.Println("\nAttempting to call SayHello after logout...")
	err = client.SayHello("World")
	if err != nil {
		log.Printf("Expected error after logout: %v", err)
	}

	// Test with invalid credentials
	log.Println("\nTesting with invalid credentials...")
	client2, err := NewAuthClient()
	if err != nil {
		log.Fatalf("Failed to create client2: %v", err)
	}
	defer client2.Close()

	err = client2.Login("wrong", "credentials")
	if err != nil {
		log.Printf("Expected login error: %v", err)
	}
}
