package stream

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"sync"
)

//Segment stores the buffer
type Segment struct {
	log  *os.File
	name string
	path string
	lock sync.Mutex
}

// Marshal the segment
var Marshal = func(v interface{}) (io.Reader, error) {
	b, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(b), nil
}

//Unmarshal the segment
var Unmarshal = func(r io.Reader, v interface{}) error {
	return json.NewDecoder(r).Decode(v)
}

//NewSegment creates a new segment
func NewSegment(name string, path string, args ...Message) *Segment {
	s := &Segment{
		name: name,
		path: path,
	}
	return s
}

//CommitToFile stores the messages to file
func (sg *Segment) CommitToFile(messages []Message) error {
	sg.lock.Lock()
	defer sg.lock.Unlock()

	file, err := os.OpenFile(sg.path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	defer file.Close()
	for _, msg := range messages {
		r, err := Marshal(msg)
		if err != nil {
			return err
		}
		_, err = io.Copy(file, r)
	}
	return err
}

/* Load loads the file at path into v.
   Use os.IsNotExist() to see if the returned error is due
   to the file being missing.
*/
/* func (sg *Segment) Load(path string) error {

	file, err := os.Open("/path/to/file.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		text := scanner.Text()
		fmt.Println(text)
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	return err
} */
