package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/zukigit/learn-gRPC/greet"
	pb "github.com/zukigit/learn-gRPC/greet"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	port = ":50051"
	// JWT secret key (in production, use environment variable)
	jwtSecret = "your-secret-key-should-be-32-chars-minimum"
)

// Custom claims structure
type CustomClaims struct {
	Username string `json:"username"`
	Role     string `json:"role"`
	jwt.RegisteredClaims
}

type server struct {
	pb.UnimplementedGreeterServer
	mu         sync.RWMutex
	userStore  map[string]User // username -> User struct
	jwtSecret  []byte
	tokenStore map[string]bool // token blacklist (for logout)
}

type User struct {
	Password string
	Role     string
}

// authInterceptor is a server interceptor that performs JWT authentication
func authInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	// Skip authentication for Login method
	if info.FullMethod == greet.Greeter_Login_FullMethodName ||
		info.FullMethod == greet.Greeter_Register_FullMethodName {
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

	// Extract token from "Bearer <token>" format
	tokenStr := values[0]
	if len(tokenStr) > 7 && tokenStr[:7] == "Bearer " {
		tokenStr = tokenStr[7:]
	}

	// Get server instance
	s := getServerFromContext(ctx)
	if s == nil {
		return nil, status.Errorf(codes.Internal, "server not found")
	}

	// Validate JWT token
	claims, err := s.validateJWT(tokenStr)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "invalid token: %v", err)
	}

	// Check if token is blacklisted (logged out)
	if s.isTokenBlacklisted(tokenStr) {
		return nil, status.Errorf(codes.Unauthenticated, "token has been revoked")
	}

	// Add claims to context for use in handlers
	ctx = context.WithValue(ctx, "claims", claims)

	log.Printf("Authenticated user: %s for method: %s", claims.Username, info.FullMethod)
	return handler(ctx, req)
}

// streamAuthInterceptor for streaming RPCs
func streamAuthInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	// Skip authentication for streaming methods that don't require auth
	if info.FullMethod == greet.Greeter_SayHelloStream_FullMethodName {
		return handler(srv, ss)
	}

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

	// Extract token
	tokenStr := values[0]
	if len(tokenStr) > 7 && tokenStr[:7] == "Bearer " {
		tokenStr = tokenStr[7:]
	}

	// Get server instance
	s := getServerFromContext(ss.Context())
	if s == nil {
		return status.Errorf(codes.Internal, "server not found")
	}

	// Validate JWT token
	claims, err := s.validateJWT(tokenStr)
	if err != nil {
		return status.Errorf(codes.Unauthenticated, "invalid token: %v", err)
	}

	// Check if token is blacklisted
	if s.isTokenBlacklisted(tokenStr) {
		return status.Errorf(codes.Unauthenticated, "token has been revoked")
	}

	log.Printf("Authenticated user: %s for streaming method: %s", claims.Username, info.FullMethod)

	// Add claims to stream context
	ctx := context.WithValue(ss.Context(), "claims", claims)
	wrappedStream := &serverStream{
		ServerStream: ss,
		ctx:          ctx,
	}

	return handler(srv, wrappedStream)
}

// Helper function to get server from context
func getServerFromContext(ctx context.Context) *server {
	if s, ok := ctx.Value("server").(*server); ok {
		return s
	}
	return nil
}

// Generate JWT token
func (s *server) generateJWT(username, role string) (string, error) {
	expirationTime := time.Now().Add(24 * time.Hour)

	claims := &CustomClaims{
		Username: username,
		Role:     role,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(expirationTime),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			Issuer:    "greet-server",
			Subject:   username,
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, err := token.SignedString(s.jwtSecret)
	if err != nil {
		return "", err
	}

	return tokenString, nil
}

// Validate JWT token
func (s *server) validateJWT(tokenString string) (*CustomClaims, error) {
	token, err := jwt.ParseWithClaims(tokenString, &CustomClaims{}, func(token *jwt.Token) (interface{}, error) {
		// Validate signing method
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return s.jwtSecret, nil
	})

	if err != nil {
		return nil, err
	}

	if claims, ok := token.Claims.(*CustomClaims); ok && token.Valid {
		return claims, nil
	}

	return nil, fmt.Errorf("invalid token claims")
}

// Add token to blacklist (for logout functionality)
func (s *server) blacklistToken(token string, expiry time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Only store token for its remaining lifetime
	go func() {
		time.Sleep(time.Until(expiry))
		s.mu.Lock()
		delete(s.tokenStore, token)
		s.mu.Unlock()
	}()

	s.tokenStore[token] = true
}

// Check if token is blacklisted
func (s *server) isTokenBlacklisted(token string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.tokenStore[token]
}

// Login implements the Login RPC
func (s *server) Login(ctx context.Context, in *pb.LoginRequest) (*pb.LoginResponse, error) {
	s.mu.RLock()
	user, exists := s.userStore[in.Username]
	s.mu.RUnlock()

	if !exists || user.Password != in.Password {
		return nil, status.Errorf(codes.Unauthenticated, "invalid username or password")
	}

	// Generate JWT token
	token, err := s.generateJWT(in.Username, user.Role)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to generate token")
	}

	expirationTime := time.Now().Add(24 * time.Hour)
	log.Printf("User %s logged in, JWT token generated", in.Username)

	return &pb.LoginResponse{
		Token:     token,
		ExpiresAt: expirationTime.Unix(),
	}, nil
}

// Register implements a new Register RPC
func (s *server) Register(ctx context.Context, in *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.userStore[in.Username]; exists {
		return nil, status.Errorf(codes.AlreadyExists, "username already exists")
	}

	// In production, hash the password!
	s.userStore[in.Username] = User{
		Password: in.Password,
		Role:     "user", // Default role
	}

	log.Printf("User %s registered successfully", in.Username)
	return &pb.RegisterResponse{
		Success: true,
		Message: "User registered successfully",
	}, nil
}

// Logout implements logout functionality
func (s *server) Logout(ctx context.Context, in *pb.LogoutRequest) (*pb.LogoutResponse, error) {
	// Extract token from context
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Errorf(codes.Unauthenticated, "missing metadata")
	}

	values := md.Get("authorization")
	if len(values) == 0 {
		return nil, status.Errorf(codes.Unauthenticated, "missing authorization token")
	}

	tokenStr := values[0]
	if len(tokenStr) > 7 && tokenStr[:7] == "Bearer " {
		tokenStr = tokenStr[7:]
	}

	// Parse token to get expiry
	claims, err := s.validateJWT(tokenStr)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "invalid token")
	}

	// Add token to blacklist
	expiry := claims.ExpiresAt.Time
	s.blacklistToken(tokenStr, expiry)

	return &pb.LogoutResponse{
		Success: true,
		Message: "Logged out successfully",
	}, nil
}

// GetProfile - example of a protected method using JWT claims
func (s *server) GetProfile(ctx context.Context, in *pb.ProfileRequest) (*pb.ProfileResponse, error) {
	// Get claims from context
	claims, ok := ctx.Value("claims").(*CustomClaims)
	if !ok {
		return nil, status.Errorf(codes.Unauthenticated, "no user claims found")
	}

	return &pb.ProfileResponse{
		Username: claims.Username,
		Role:     claims.Role,
		Message:  fmt.Sprintf("Profile for %s (role: %s)", claims.Username, claims.Role),
	}, nil
}

// SayHello implements the Greeter service
func (s *server) SayHello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	claims, ok := ctx.Value("claims").(*CustomClaims)
	if ok {
		log.Printf("Received from user %s: %v", claims.Username, in.GetName())
		return &pb.HelloReply{Message: fmt.Sprintf("Hello %s (authenticated as %s)", in.GetName(), claims.Username)}, nil
	}

	log.Printf("Received: %v", in.GetName())
	return &pb.HelloReply{Message: "Hello " + in.GetName()}, nil
}

// SayHelloStream implements the streaming RPC
func (s *server) SayHelloStream(in *pb.HelloRequest, stream pb.Greeter_SayHelloStreamServer) error {
	claims, ok := stream.Context().Value("claims").(*CustomClaims)
	userName := "Guest"
	if ok {
		userName = claims.Username
	}

	log.Printf("Streaming response for: %v (user: %s)", in.GetName(), userName)

	for i := 0; i < 5; i++ {
		message := &pb.HelloReply{
			Message: fmt.Sprintf("Hello %s, message #%d from %s", in.GetName(), i+1, userName),
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

	// Create server with JWT
	srv := &server{
		userStore:  make(map[string]User),
		jwtSecret:  []byte(jwtSecret),
		tokenStore: make(map[string]bool),
	}

	// Add some test users
	srv.userStore["admin"] = User{Password: "admin123", Role: "admin"}
	srv.userStore["user"] = User{Password: "user123", Role: "user"}

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
	log.Printf("JWT Secret: %s...", jwtSecret[:10])

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
