package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
)

type Broker struct {
	ID          int64
	addressPort string
}

func NewBroker(id int64, address string, r *Registry) *Broker {
	return &Broker{
		ID:          id,
		addressPort: address,
	}
}

func (b *Broker) Start() error {
	ln, err := net.Listen("tcp", b.addressPort)
	if err != nil {
		panic(err)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			panic(err)
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)

	for {
		msg, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				log.Println("Error reading:", err)
			}
			break
		}
		fmt.Println("Received:", msg)

		// Respond to the client
		response := "Message received\n"
		conn.Write([]byte(response))
	}
}

func main() {
	r := NewRegistry()
	b := NewBroker(1, ":5000", r)
	b.Start()

	
}
