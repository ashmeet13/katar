package segment

import (
	"encoding/binary"
	"fmt"
	"os"
	"time"

	"github.com/ashmeet13/katar/internal/log"
)

type Segment struct {
	BaseOffset    int64
	Offset        int64
	KatarFile     string
	IndexFile     string
	TimeIndexFile string
}

func NewSegment(baseOffset int64, directory string) *Segment {
	return &Segment{
		BaseOffset:    baseOffset,
		Offset:        0,
		KatarFile:     fmt.Sprintf("%s/%v.katar", directory, baseOffset),
		IndexFile:     fmt.Sprintf("%s/%v.index", directory, baseOffset),
		TimeIndexFile: fmt.Sprintf("%s/%v.timeindex", directory, baseOffset),
	}
}

func (s *Segment) Write(payload []byte) {
	log := log.NewLog(payload, s.BaseOffset, s.Offset, time.Now().Unix())
	serializedLog, err := log.Serialize()
	if err != nil {
		panic(err)
	}

	logLen := len(serializedLog)
	logLenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(logLenBytes, uint32(logLen))

	file, err := os.OpenFile(s.KatarFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	bytesToWrite := append(logLenBytes, serializedLog...)
	file.Write(bytesToWrite)
	s.Offset += 1
}

func (s *Segment) PrintSegment() {
	file, err := os.Open(s.KatarFile)
	if err != nil {
		panic(err)
	}

	for {
		logLenBytes := make([]byte, 4)
		_, err := file.Read(logLenBytes)
		fmt.Println(logLenBytes, err)
		if err != nil {
			panic((err))
		}

		logLen := int32(binary.BigEndian.Uint32(logLenBytes))
		logBytes := make([]byte, logLen)

		_, err = file.Read(logBytes)
		if err != nil {
			panic((err))
		}

		log := log.NewLog(nil, 0, 0, 0)
		log.DeSerialize(logBytes)

		log.PrintLog()
	}
}
