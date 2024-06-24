package server

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
)

func handleConnection(conn net.Conn) {
	log.Println("handling connection")
	var buf bytes.Buffer
	_, err := io.Copy(&buf, conn)
	if err != nil {
		log.Printf("failed to read from connection, %v\n", err)
	}
	fmt.Println("total size:", buf.Len())
	conn.Write(buf.Bytes())
}

func Start() {
	log.Println("starting server")
	ln, err := net.Listen("tcp", ":2222")
	if err != nil {
		log.Fatalf("unable to start server, %v", err)
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("unable to create new connection, %v\n", err)
		}
		go handleConnection(conn)
	}
}
