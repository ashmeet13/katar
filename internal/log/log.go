package log

import (
	"encoding/json"
	"fmt"
)

type Log struct {
	BaseOffset int64
	Offset     int64
	Timestamp  int64
	Payload    []byte
}

func NewLog(payload []byte, baseOffset, offset, timestamp int64) *Log {
	return &Log{
		Payload:    payload,
		BaseOffset: baseOffset,
		Offset:     offset,
		Timestamp:  timestamp,
	}
}

func (l *Log) Serialize() ([]byte, error) {
	return json.Marshal(l)
}

func (l *Log) DeSerialize(logBytes []byte) error {
	return json.Unmarshal(logBytes, l)
}

func (l *Log) PrintLog() {
	fmt.Printf("BaseOffset: %v, Offset: %v, Timestamp: %v, Payload: %s", l.BaseOffset, l.Offset, l.Timestamp, string(l.Payload))
}
