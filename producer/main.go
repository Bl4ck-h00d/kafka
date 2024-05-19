package main

import (
	"encoding/json"
	"log"
	"net"
	"time"
)

type Message struct {
	Type    string // "produce", "consume", "register_producer", "register_consumer", "create_topic"
	Topic   string // Topic name
	Message []byte // Message for produce
	ID      string  // Producer/Consumer ID
	Group   string // Consumer group
}

const brokerAddress = "localhost:5000"

func sendMessage(action string, id string, topic string, message string) {
	// Create a connection to the broker
	conn, err := net.Dial("tcp", brokerAddress)
	if err != nil {
		log.Fatalf("Failed to connect to broker: %v", err)
	}
	defer conn.Close()

	// Prepare the protocol message
	protocolMsg := Message{
		Type:    action,
		Topic:   topic,
		ID:      id,
		Message: []byte(message),
	}

	// Serialize the message to JSON
	msg, err := json.Marshal(protocolMsg)
	if err != nil {
		log.Fatalf("Failed to marshal message: %v", err)
	}

	// Send the message to the broker
	_, err = conn.Write(append(msg, '\n'))
	if err != nil {
		log.Fatalf("Failed to send message: %v", err)
	}
}

func main() {
	// Loop to send messages
	for {
		// Register the producer
		sendMessage("register_producer", "consumer_1", "topic1", "")

		// Produce a message
		sendMessage("produce", "consumer_1", "topic1", "Hi there")

		// Wait for 2 seconds before sending the next message
		time.Sleep(5 * time.Second)
	}
}
