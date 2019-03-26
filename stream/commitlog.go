package stream

import (
	"os"
)

const (
	path = "/home/souvik/iris/github.com/sovikc/iris/logs/"
)

// CreateTopicDirectory creates the topic directory on the disk
func (t *Topic) CreateTopicDirectory() error {
	location := path + t.Name
	err := os.MkdirAll(location, 0755)
	return err
}

// CreatePartitionFile creates partition file on the disk
func (prt *Partition) CreatePartitionFile() error {
	prt.Path = path + prt.Topic + "/" + prt.Name + ".log"
	f, err := os.Create(prt.Path)
	defer f.Close()
	return err
}

//WriteToLog writes to log
func WriteToLog(stream *Stream, status int, dir string, file string, logErr chan<- error) {

	if status == FULL {
		stream.Status = LOCKED
		path := path + dir + "/" + file + ".log"
		segment := NewSegment(stream.Name, path)
		err := segment.CommitToFile(stream.Messages)
		if err != nil {
			logErr <- err
			return
		}

		stream.Messages = stream.Messages[:0]
		stream.Status = READY
	}
	logErr <- nil
}
