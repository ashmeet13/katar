package main

import (
	"log"
	"net"

	"github.com/ashmeet13/katar/katar/pb"
	"github.com/ashmeet13/katar/katar/server"
	"google.golang.org/grpc"
)

func main() {
	log.Println("Starting Katar")

	listener, err := net.Listen("tcp", "localhost:3110")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterKatarServer(grpcServer, server.NewKatarServer())

	grpcServer.Serve(listener)
}
