package client

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
)

func SendRequest() {
	conn, err := net.Dial("tcp", ":2222")
	if err != nil {
		log.Printf("failed to establish connection to server, %v\n", err)
		return
	}
	defer conn.Close()
	log.Println("sending msg to server")
	msg := []byte("hello world\n")
	n, err := fmt.Fprintf(conn, string(msg))
	if err != nil {
		log.Printf("failed to write to server, %v\n", err)
		return
	}
	fmt.Println("WROTE MSG")
	if n != len(msg) {
		log.Printf("didn't write full msg, tried to write %d bytes, only wrote %d\n", len(msg), n)
		return
	}
	var buf bytes.Buffer
	io.Copy(&buf, conn)
	fmt.Println("total size:", buf.Len())
	log.Printf("rcvd msg from server: \n%s\n", string(buf.Bytes()))
}

// in the future will have functions for Get and Put
