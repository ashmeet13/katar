package server

import (
	"context"
	"log"

	"github.com/ashmeet13/katar/internal/topic"
	"github.com/ashmeet13/katar/katar/config"
	"github.com/ashmeet13/katar/katar/pb"
)

type KatarServer struct {
	Config *config.Config
	pb.UnimplementedKatarServer
}

func NewKatarServer() *KatarServer {
	return &KatarServer{
		Config: config.NewConfig(),
	}
}

func (s *KatarServer) Publish(ctx context.Context, request *pb.PublishRequest) (*pb.PublishResponse, error) {
	log.Printf("Writing to topic %s\n", request.Topic)
	topic.NewTopic(request.Topic, s.Config).Write(request.Payload)

	return &pb.PublishResponse{
		Response: &pb.PublishResponse_Success{},
	}, nil
}
