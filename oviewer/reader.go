package oviewer

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync/atomic"

	"golang.org/x/term"
)

const FormFeed = "\f"

type fileRead struct {
	// File is the os.File.
	file *os.File
	// CFormat is a compressed format.
	CFormat Compressed

	cancel context.CancelFunc

	// offset
	offset int64
	// 1 if EOF is reached.
	eof int32
	// notif when eof is reached.
	eofCh chan struct{}
	// 1 if there is a closed.
	closed int32
	// notify when reopening.
	followCh chan struct{}
	// openFollow represents the open followMode file.
	openFollow int32
	// 1 if there is a changed.
	changed int32
	// notify when a file changes.
	changCh chan struct{}
	// preventReload is true to prevent reload.
	preventReload bool
	// Is it possible to seek.
	seekable bool
}

func NewFileRead() *fileRead {
	return &fileRead{
		eofCh:         make(chan struct{}),
		followCh:      make(chan struct{}),
		changCh:       make(chan struct{}),
		seekable:      true,
		preventReload: false,
	}
}

// ReadFile reads file.
// If the file name is empty, read from standard input.
func (m *Document) ReadFile(fileName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	f, err := open(fileName)
	if err != nil {
		return err
	}
	m.reader.file = f
	m.FileName = fileName

	cFormat, r := uncompressedReader(m.reader.file)
	m.reader.CFormat = cFormat

	go m.waitEOF()

	if STDOUTPIPE != nil {
		r = io.TeeReader(r, STDOUTPIPE)
	}

	return m.ReadAll(r)
}

func open(fileName string) (*os.File, error) {
	if fileName == "" {
		if term.IsTerminal(0) {
			return nil, ErrMissingFile
		}
		return os.Stdin, nil
	}

	return os.Open(fileName)
}

// waitEOF waits until EOF is reached before closing.
func (m *Document) waitEOF() {
	<-m.reader.eofCh
	if m.reader.seekable {
		if err := m.close(); err != nil {
			log.Printf("EOF: %s", err)
		}
	}
	m.change()
	m.reader.followCh <- struct{}{}
}

// ReadReader reads reader.
// A wrapper for ReadAll, used when eofCh notifications are not needed.
func (m *Document) ReadReader(r io.Reader) error {
	go func() {
		<-m.reader.eofCh
	}()

	return m.ReadAll(r)
}

// ReadAll reads all from the reader.
// And store it in the lines of the Document.
// ReadAll needs to be notified on eofCh.
func (m *Document) ReadAll(r io.Reader) error {
	reader := bufio.NewReader(r)
	go func() {
		if m.checkClose() {
			return
		}

		if err := m.readAll(reader); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) || errors.Is(err, os.ErrClosed) {
				m.reader.eofCh <- struct{}{}
				atomic.StoreInt32(&m.reader.eof, 1)
				return
			}
			log.Printf("error: %v\n", err)
			atomic.StoreInt32(&m.reader.eof, 0)
			return
		}
	}()

	// Named pipes for continuous read.
	if !m.reader.seekable {
		m.onceFollowMode()
	}
	return nil
}

// onceFollowMode opens the follow mode only once.
func (m *Document) onceFollowMode() {
	if atomic.SwapInt32(&m.reader.openFollow, 1) == 1 {
		return
	}
	if m.reader.file == nil {
		return
	}

	var cancel context.CancelFunc
	ctx := context.Background()
	ctx, cancel = context.WithCancel(ctx)
	go m.startFollowMode(ctx, cancel)
	m.reader.cancel = cancel
}

// startFollowMode opens the file in follow mode.
// Seek to the position where the file was closed, and then read.
func (m *Document) startFollowMode(ctx context.Context, cancel context.CancelFunc) {
	defer cancel()
	<-m.reader.followCh
	if m.reader.seekable {
		// Wait for the file to open until it changes.
		select {
		case <-ctx.Done():
			return
		case <-m.reader.changCh:
		}
		m.reader.file = m.openFollowFile()
	}

	r := compressedFormatReader(m.reader.CFormat, m.reader.file)
	if err := m.ContinueReadAll(ctx, r); err != nil {
		log.Printf("%s follow mode read %v", m.FileName, err)
	}
}

// openFollowFile opens a file in follow mode.
func (m *Document) openFollowFile() *os.File {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, err := os.Open(m.FileName)
	if err != nil {
		log.Printf("openFollowFile: %s", err)
		return m.reader.file
	}
	atomic.StoreInt32(&m.reader.closed, 0)
	atomic.StoreInt32(&m.reader.eof, 0)
	if _, err := r.Seek(m.reader.offset, io.SeekStart); err != nil {
		log.Printf("openFollowMode: %s", err)
	}
	return r
}

// ContinueReadAll continues to read even if it reaches EOF.
func (m *Document) ContinueReadAll(ctx context.Context, r io.Reader) error {
	reader := bufio.NewReader(r)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if m.checkClose() {
			return nil
		}

		if err := m.readAll(reader); err != nil {
			if errors.Is(err, io.EOF) {
				<-m.reader.changCh
				continue
			}
			return err
		}
	}
}

// readAll actually reads everything.
// The read lines are stored in the lines of the Document.
func (m *Document) readAll(reader *bufio.Reader) error {
	var line strings.Builder

	for {
		buf, isPrefix, err := reader.ReadLine()
		if err != nil {
			return err
		}
		line.Write(buf)
		if isPrefix {
			continue
		}

		m.append(line.String())
		line.Reset()
	}
}

// append appends to the lines of the document.
func (m *Document) append(lines ...string) {
	m.mu.Lock()
	for _, line := range lines {
		m.lines = append(m.lines, line)
		m.endNum++
	}
	m.mu.Unlock()
	m.change()
}

func (m *Document) appendFormFeed() {
	line := ""
	m.mu.Lock()
	if m.endNum > 0 {
		line = m.lines[m.endNum-1]
	}
	m.mu.Unlock()

	// Do not add if the previous is FormFeed.
	if line != FormFeed {
		m.append(FormFeed)
	}
}

// reload will read again.
// Regular files are reopened and reread increase.
// The pipe will reset what it has read.
func (m *Document) reload() error {
	if m.reader.preventReload {
		return fmt.Errorf("not file: %s", m.FileName)
	}

	if (m.reader.file == os.Stdin && m.BufEOF()) || !m.reader.seekable && m.reader.checkClose() {
		return fmt.Errorf("%w %s", ErrAlreadyClose, m.FileName)
	}

	if m.reader.seekable {
		if m.reader.cancel != nil {
			m.reader.cancel()
		}
		if !m.reader.checkClose() && m.reader.file != nil {
			if err := m.reader.close(); err != nil {
				log.Println(err)
			}
		}
	}

	if m.WatchMode {
		m.appendFormFeed()
	} else {
		m.reset()
		m.topLN = 0
	}

	if !m.reader.seekable {
		return nil
	}

	atomic.StoreInt32(&m.reader.closed, 0)
	return m.ReadFile(m.FileName)
}

// reset clears all lines.
func (m *Document) reset() {
	m.mu.Lock()
	m.endNum = 0
	m.lines = m.lines[:0]
	m.mu.Unlock()

	m.change()
	m.ClearCache()
}

// Close closes the File.
// Record the last read position.
func (r *fileRead) close() error {
	if r.checkClose() {
		return nil
	}

	if r.seekable {
		pos, err := r.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return fmt.Errorf("close: %w", err)
		}
		r.offset = pos
	}
	if err := r.file.Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}
	atomic.StoreInt32(&r.openFollow, 0)
	atomic.StoreInt32(&r.closed, 1)
	r.change()

	return nil
}

// checkClose returns if the file is closed.
func (r *fileRead) checkClose() bool {
	return atomic.LoadInt32(&r.closed) == 1
}

func (r *fileRead) checkChangeAndReset() bool {
	return atomic.SwapInt32(&r.changed, 0) == 1
}

func (r *fileRead) change() {
	atomic.StoreInt32(&r.changed, 1)
}
