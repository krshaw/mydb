package database

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
)

const (
	END = 2
)

// TODO: create a freelist

type MyDBClient struct {
	store *os.File
}

func New(path string) (*MyDBClient, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		log.Printf("failed to create tmp file for testing, %v\n", err)
		return nil, err
	}
	return &MyDBClient{f}, nil
}

func (c *MyDBClient) Close() error {
	return c.store.Close()
}

func (c *MyDBClient) Put(key, value []byte) error {
	// write to the end of the file
	// TOOD: handle case where key already exists
	// when the key already exists, check if we can fit the new value in the current block
	// if not, mark the entry as free and add it to the freelist (free list contains offset and space available in entry)
	// so now the entry saved on disk will have another byte saying it is free,
	// and there will be a different entry for that key with the new value
	// then fallthrough to the common case, where we would be if the key was new
	// search the freelist for an entry first, then if one with enough space doesn't exist, append to the datafile
	found, off, err := c.find(key)
	if err != nil {
		log.Printf("failed to find key %v if it already exists\n", key)
		return err
	}
	var buf bytes.Buffer
	if found {
		meta := make([]byte, 9)
		_, err := c.store.ReadAt(meta, off)
		if err != nil && err != io.EOF {
			log.Printf("failed to Put key %v, error %v\n", key, err)
			return err
		}
		// don't need to read the key, can skip past the key length
		var keyLength int32
		err = binary.Read(bytes.NewReader(meta[:4]), binary.BigEndian, &keyLength)
		if err != nil {
			log.Printf("failed to read length prefix for key %v, error %v\n", key, err)
			return err
		}
		var valueLength int32
		err = binary.Read(bytes.NewReader(meta[4:8]), binary.BigEndian, &valueLength)
		if err != nil {
			log.Printf("failed to read length prefix for key %v, error %v\n", key, err)
			return err
		}
		if valueLength <= int32(len(value)) {
			// off + 4 is start of value length
			err = binary.Write(&buf, binary.BigEndian, int32(len(value)))
			if err != nil {
				log.Println("failed to write length prefix of value")
				return err
			}
			_, err = c.store.WriteAt(buf.Bytes(), off+4)
			if err != nil {
				log.Printf("failed to write new value length for key %v\n", key)
				return err
			}
			// off + 4 + 4 + 1 + keylength is where value starts
			_, err = c.store.WriteAt(value, off+9+int64(keyLength))
			if err != nil {
				log.Printf("failed to write new value %v for key %v\n", value, key)
				return err
			}
			return nil
			// in the case where new value length is < old value length,
			// there will be a gap between the the modified value and the next entry
			// this means the current scheme of assuming every entry is immediately after the other is broken
			// TODO: the meta block needs to contain a pointer to the next entry
			// update the value length and value
		}
		// TODO: else fallthrough to common case and mark this entry as free
	}
	// adds an extra 4 bytes
	// TODO: shouldn't be casting this to an int32. int64 is probably better, but should just get the exact size with unsafe.Sizeof
	err = binary.Write(&buf, binary.BigEndian, int32(len(key)))
	if err != nil {
		log.Println("failed to write length prefix of key")
		return err
	}
	// adds an extra 4 bytes
	err = binary.Write(&buf, binary.BigEndian, int32(len(value)))
	if err != nil {
		log.Println("failed to write length prefix of value")
		return err
	}
	// also write a byte indicating this entry is not free
	buf.WriteByte(0)
	buf.Write(key)
	buf.Write(value)
	// panic if the length of the buffer is not len(key)+len(value) + 8 (8 coming from the two length prefixes)
	expectedLen := len(key) + len(value) + 9
	if buf.Len() != expectedLen {
		log.Panicf("buffer being written to db is incorrect, expected %d bytes, got %d\n", buf.Len(), expectedLen)
	}
	off, err = c.store.Seek(0, END)
	if err != nil {
		log.Printf("failed to Put key %v, value %v, error %v", key, value, err)
		return err
	}
	_, err = c.store.WriteAt(buf.Bytes(), off)
	return err
}

func (c *MyDBClient) Get(key []byte) ([]byte, error) {
	if found, off, err := c.find(key); err != nil {
		return nil, fmt.Errorf("failed to Get key %v", err)
	} else if !found {
		return nil, fmt.Errorf("key %v does not exist", key)
	} else {
		meta := make([]byte, 9)
		n, err := c.store.ReadAt(meta, off)
		if err != nil && err != io.EOF {
			log.Printf("failed to Get key %v, error %v\n", key, err)
			return nil, err
		}
		off += int64(n)
		// don't need to read the key, can skip past the key length
		var keyLength int32
		err = binary.Read(bytes.NewReader(meta[:4]), binary.BigEndian, &keyLength)
		if err != nil {
			log.Printf("failed to read length prefix for key %v, error %v\n", key, err)
			return nil, err
		}
		off += int64(keyLength)
		var valueLength int32
		err = binary.Read(bytes.NewReader(meta[4:8]), binary.BigEndian, &valueLength)
		if err != nil {
			log.Printf("failed to read length prefix for key %v, error %v\n", key, err)
			return nil, err
		}
		value := make([]byte, valueLength)
		_, err = c.store.ReadAt(value, off)
		if err != nil {
			return nil, fmt.Errorf("found key %v but unable to read value", key)
		}
		return value, nil
	}
}

// find returns the byte offset of the entry for key, if it exists
// the first return value specifies if the key exists in the database
func (c *MyDBClient) find(key []byte) (bool, int64, error) {
	// TODO: have this function return a struct so the caller doesn't have to read the length prefix from disk again
	// struct should contain: offset, keylength, valuelength
	off := int64(0)
	meta := make([]byte, 9)
	for {
		// only need 8 bytes to read the length prefix
		n, err := c.store.ReadAt(meta, off)
		if err != nil && err != io.EOF {
			log.Printf("failed to Get key %v, error %v\n", key, err)
			return false, 0, err
		}
		// before moving offset past the length prefix, save it incase this is the entry for key
		currEntryOff := off
		if n > 0 {
			off += int64(n)
			var keyLength int32
			err = binary.Read(bytes.NewReader(meta[:4]), binary.BigEndian, &keyLength)
			if err != nil {
				log.Printf("failed to read length prefix for key %v, error %v\n", key, err)
				return false, 0, err
			}
			var valueLength int32
			err = binary.Read(bytes.NewReader(meta[4:8]), binary.BigEndian, &valueLength)
			if err != nil {
				log.Printf("failed to read length prefix for key %v, error %v\n", key, err)
				return false, 0, err
			}
			freed := meta[8]
			if freed == 0 {
				// read keyLength bytes into a buffer for the key
				currKey := make([]byte, keyLength)
				_, err = c.store.ReadAt(currKey, off)
				if err != nil {
					log.Printf("failed to Get key %v, error %v\n", key, err)
					return false, 0, err
				}
				if bytes.Equal(key, currKey) {
					return true, currEntryOff, nil
				}

			}
			// else move offset past value and continue
			off += int64(keyLength + valueLength)
		}
		if err == io.EOF {
			return false, 0, nil
		}
	}
}
