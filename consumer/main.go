package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
)

func receiveMessage() string {
	conn, err := net.Dial("tcp", "localhost:5000")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	serverReader := bufio.NewReader(conn)

	for {
		response, err := serverReader.ReadString('\n')

		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("Server response: %s\n", response)
	}
}

func main() {
    receiveMessage()
}
