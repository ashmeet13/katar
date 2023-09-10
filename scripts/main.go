package main

import (
	"github.com/ashmeet13/katar/internal/segment"
)

func main() {
	topicDir := "/tmp/katar/test_katar"

	segment := segment.NewSegment(0, topicDir)

	segment.PrintSegment()
}
