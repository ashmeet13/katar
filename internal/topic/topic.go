package topic

import (
	"os"

	"github.com/ashmeet13/katar/internal/segment"
	"github.com/ashmeet13/katar/katar/config"
)

type Topic struct {
	Name          string
	TopicDir      string
	ActiveSegment *segment.Segment
}

func NewTopic(topicName string, config *config.Config) *Topic {
	topicDir := initaliseTopicDirectory(topicName, config.KatarDir)

	return &Topic{
		Name:          topicName,
		TopicDir:      topicDir,
		ActiveSegment: segment.NewSegment(0, topicDir),
	}
}

func initaliseTopicDirectory(topicName, baseDir string) string {
	topicDir := baseDir + "/" + topicName
	err := os.MkdirAll(topicDir, os.ModePerm)
	if err != nil {
		panic(err)
	}
	return topicDir
}

func (t *Topic) Write(payload []byte) {
	t.ActiveSegment.Write(payload)
}
