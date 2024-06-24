package database

import (
	"bytes"
	"testing"
)

func TestPutAndGet(t *testing.T) {
	c, err := New("test-data")
	if err != nil {
		t.Fatalf("failed to create db, %v\n", err)
	}
	defer c.Close()
	err = c.Put([]byte("hello"), []byte("world"))
	if err != nil {
		t.Fatalf("failed to Put to db, %v\n", err)
	}
	// make sure data exists
	got, err := c.Get([]byte("hello"))
	if err != nil {
		t.Fatalf("failed to Get from db, %v\n", err)
	}
	want := []byte("world")
	if !bytes.Equal(got, want) {
		t.Fatalf("got: %v but wanted: %v, for Get(%v)\n", got, want, []byte("hello"))
	}
	// TODO: add more test cases
}

func TestPutUpdate(t *testing.T) {
	// TODO: add test cases for when new value is different size (bigger or smaller)
	c, err := New("test-data")
	if err != nil {
		t.Fatalf("failed to create db, %v\n", err)
	}
	defer c.Close()
	key := []byte("hello")
	err = c.Put(key, []byte("world"))
	if err != nil {
		t.Fatalf("failed to Put to db, %v\n", err)
	}
	err = c.Put(key, []byte("there"))
	if err != nil {
		t.Fatalf("failed to update key in db, %v\n", err)
	}
	got, err := c.Get(key)
	if err != nil {
		t.Fatalf("failed to Get from db, %v\n", err)
	}
	want := []byte("there")
	if bytes.Equal(got, []byte("world")) {
		t.Fatalf("got old value %v for key %v, expected to get new value %v\n", got, key, want)
	} else if !bytes.Equal(got, want) {
		t.Fatalf("got: %v but wanted: %v, for Get(%v)\n", got, want, key)
	}
}
