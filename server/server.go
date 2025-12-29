package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	pb "github.com/zukigit/learn-gRPC/greet"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	port        = ":50051"
	tokenLength = 32
	tokenExpiry = 24 * time.Hour
)

type server struct {
	pb.UnimplementedGreeterServer
	tokens    map[string]time.Time
	tokensMu  sync.RWMutex
	userStore map[string]string // username -> password (in real app, use hashed passwords!)
}

// authInterceptor is a server interceptor that performs authentication
func authInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	// Skip authentication for Login method
	if info.FullMethod == "/greet.Greeter/Login" {
		return handler(ctx, req)
	}

	// Extract metadata from the context
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Errorf(codes.Unauthenticated, "missing metadata")
	}

	// Check for the "authorization" header
	values := md.Get("authorization")
	if len(values) == 0 {
		return nil, status.Errorf(codes.Unauthenticated, "missing authorization token")
	}

	// Validate token
	token := values[0]
	s := getServerFromContext(ctx)
	if s == nil {
		return nil, status.Errorf(codes.Internal, "server not found")
	}

	if !s.isValidToken(token) {
		return nil, status.Errorf(codes.Unauthenticated, "invalid or expired token")
	}

	// If authorized, proceed with the main handler logic
	log.Printf("Authenticated user for method: %s", info.FullMethod)
	return handler(ctx, req)
}

// streamAuthInterceptor for streaming RPCs
func streamAuthInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	// Skip authentication for Login method (though Login isn't streaming)
	// This example shows how to handle streaming methods

	// Extract metadata from the context
	md, ok := metadata.FromIncomingContext(ss.Context())
	if !ok {
		return status.Errorf(codes.Unauthenticated, "missing metadata")
	}

	// Check for the "authorization" header
	values := md.Get("authorization")
	if len(values) == 0 {
		return status.Errorf(codes.Unauthenticated, "missing authorization token")
	}

	// Validate token
	token := values[0]
	s := getServerFromContext(ss.Context())
	if s == nil {
		return status.Errorf(codes.Internal, "server not found")
	}

	if !s.isValidToken(token) {
		return status.Errorf(codes.Unauthenticated, "invalid or expired token")
	}

	log.Printf("Authenticated user for streaming method: %s", info.FullMethod)
	return handler(srv, ss)
}

// Helper function to get server from context
func getServerFromContext(ctx context.Context) *server {
	if s, ok := ctx.Value("server").(*server); ok {
		return s
	}
	return nil
}

// Login implements the Login RPC
func (s *server) Login(ctx context.Context, in *pb.LoginRequest) (*pb.LoginResponse, error) {
	// In a real application, validate against database with hashed passwords
	// This is a simplified example
	expectedPassword, exists := s.userStore[in.Username]
	if !exists || expectedPassword != in.Password {
		return nil, status.Errorf(codes.Unauthenticated, "invalid username or password")
	}

	// Generate token
	token, err := generateToken()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to generate token")
	}

	// Store token with expiry
	expiryTime := time.Now().Add(tokenExpiry)
	s.tokensMu.Lock()
	s.tokens[token] = expiryTime
	s.tokensMu.Unlock()

	// Cleanup old tokens (in production, use a proper cleanup routine)
	go s.cleanupExpiredTokens()

	log.Printf("User %s logged in, token generated", in.Username)
	return &pb.LoginResponse{
		Token:     token,
		ExpiresAt: expiryTime.Unix(),
	}, nil
}

// Helper function to validate token
func (s *server) isValidToken(token string) bool {
	s.tokensMu.RLock()
	expiry, exists := s.tokens[token]
	s.tokensMu.RUnlock()

	if !exists {
		return false
	}

	// Check if token is expired
	if time.Now().After(expiry) {
		// Remove expired token
		s.tokensMu.Lock()
		delete(s.tokens, token)
		s.tokensMu.Unlock()
		return false
	}

	return true
}

// Helper function to cleanup expired tokens
func (s *server) cleanupExpiredTokens() {
	s.tokensMu.Lock()
	defer s.tokensMu.Unlock()

	now := time.Now()
	for token, expiry := range s.tokens {
		if now.After(expiry) {
			delete(s.tokens, token)
		}
	}
}

// Helper function to generate random token
func generateToken() (string, error) {
	bytes := make([]byte, tokenLength)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
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

	// Create server with user store and token store
	srv := &server{
		tokens:    make(map[string]time.Time),
		userStore: make(map[string]string),
	}

	// Add some test users (in real app, use database)
	srv.userStore["admin"] = "admin123"
	srv.userStore["user"] = "user123"

	// Create gRPC server with interceptors
	s := grpc.NewServer(
		grpc.UnaryInterceptor(func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
			// Add server instance to context
			ctx = context.WithValue(ctx, "server", srv)
			return authInterceptor(ctx, req, info, handler)
		}),
		grpc.StreamInterceptor(func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			// Add server instance to context
			ctx := context.WithValue(ss.Context(), "server", srv)
			wrappedStream := &serverStream{
				ServerStream: ss,
				ctx:          ctx,
			}
			return streamAuthInterceptor(srv, wrappedStream, info, handler)
		}),
	)

	pb.RegisterGreeterServer(s, srv)

	log.Printf("Server listening at %v", lis.Addr())

	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

// Wrapper to add context to server stream
type serverStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (ss *serverStream) Context() context.Context {
	return ss.ctx
}
