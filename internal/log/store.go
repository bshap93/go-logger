package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian
)

const (
	lenWidth = 8
)

type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	// Get an fs.InfoFile returned which contains file length
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	// We capsulate this method with mutual exclusion lock
	s.mu.Lock()
	defer s.mu.Unlock()
	// We get the file size before appending
	pos = s.size
	// We try writing p bytes to our store buffer using enc encoding
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		// if write fail, return def
		return 0, 0, err
	}
	// w being width of number of bytes written into s buffer
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}
	// adding the number of bytes used to store the record length
	// w is size of p in bytes, lenWidth is the number of bytes used to store
	// the records length
	w += lenWidth
	// platform independent size for store
	s.size += uint64(w)

	// return the number of bytes written whichgo api convtionoally do
	return uint64(w), pos, nil
}

func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Flush the buffer, or read it, make sure what needs is there
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	// find out how many bytes we need to read to get the whole record
	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	b := make([]byte, enc.Uint64(size))
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}

	return b, nil
}

// Implements io.ReaderAt on the store type
func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.File.ReadAt(p, off)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.buf.Flush()
	if err != nil {
		return err
	}
	return s.File.Close()
}
