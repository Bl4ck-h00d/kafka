package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"time"
)

// Message structure to communicate with the broker
type Message struct {
	Type    string // "produce", "consume", "register_producer", "register_consumer", "create_topic", "join_group"
	Topic   string // Topic name
	Message []byte // Message for produce
	ID      string // Producer/Consumer ID
	Group   string // Consumer group
}

// Function to send messages to the broker and receive responses
func sendMessage(m Message) {
	conn, err := net.Dial("tcp", "localhost:5000")
	if err != nil {
		log.Fatalf("Failed to connect to broker: %v", err)
	}
	defer conn.Close()

	// Serialize the message to JSON
	data, err := json.Marshal(m)
	if err != nil {
		log.Fatalf("Failed to marshal message: %v", err)
	}

	// Send the message
	_, err = conn.Write(append(data, '\n'))
	if err != nil {
		log.Fatalf("Failed to send message: %v", err)
	}

	// Wait for a response
	serverReader := bufio.NewReader(conn)

	for {
		response, err := serverReader.ReadBytes('\n')
		if err != nil {
			log.Fatalf("Failed to read response: %v", err)
		}

		if string(response) == "--END-OF-DATA--\n" {
			break
		}

		fmt.Printf("Server response: %s\n", response)
	}
}

// Function to send a join group request
func joinGroupRequest(consumerId string, groupId string, topic string) {
	request := Message{
		Type:    "register_consumer",
		Topic:   topic,
		ID:      consumerId,
		Message: []byte(""),
		Group:   groupId,
	}
	sendMessage(request)
}

// Function to poll messages from the broker
func poll(consumerId string, groupId string, topic string) {
	request := Message{
		Type:    "consume",
		Topic:   topic,
		ID:      consumerId,
		Message: []byte(""),
		Group:   groupId,
	}
	sendMessage(request)
}

func main() {
	// Example usage: join a group and poll messages
	consumerId := "consumer1"
	groupId := "group1"
	topic := "topic1"
	log.Println("Consumer starting...")
	// joinGroupRequest(consumerId, groupId, topic)
	for {
		poll(consumerId, groupId, topic)
		time.Sleep(10 * time.Second)
	}

}
