package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
)

type Broker struct {
	ID          int64
	addressPort string
	registry    *Registry
}

type Message struct {
	Type    string // "produce", "consume", "register_producer", "register_consumer", "create_topic"
	Topic   string // Topic name
	Message []byte // Message for produce
	ID      string // Producer/Consumer ID
	Group   string // Consumer group
}

func NewBroker(id int64, address string, r *Registry) *Broker {
	return &Broker{
		ID:          id,
		addressPort: address,
		registry:    r,
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
		go handleConnection(conn, b.registry)
	}
}

func handleProduce(msg Message) error {
	err := os.MkdirAll("./data", 0755)
	if err != nil {
		return fmt.Errorf("failed to create directory: %v", err)
	}

	// Create file based on topic name
	fileName := filepath.Join("./data", msg.Topic+".log")
	log.Println("Creating: ",fileName)
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open or create file: %v", err)
	}
	defer file.Close()

	// Get the current offset
	fileInfo, err := os.Stat(fileName)
	if err != nil {
		return fmt.Errorf("failed to get file info: %v", err)
	}
	offset := fileInfo.Size()

	// Write the message with the offset
	_, err = file.WriteString(fmt.Sprintf("%d: %s\n", offset, msg.Message))
	if err != nil {
		return fmt.Errorf("failed to write message to file: %v", err)
	}

	return nil
}

func handleConsume(topic string, offset string) (string, error) {
	fileName := filepath.Join("./data", topic+".log")
	file, err := os.Open(fileName)
	if err != nil {
		return "", fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	// Seek to the specified offset
	i, _ := strconv.Atoi(offset)
	_, err = file.Seek(int64(i), io.SeekStart)
	if err != nil {
		return "", fmt.Errorf("failed to seek to offset: %v", err)
	}

	// Read from the specified offset
	reader := bufio.NewReader(file)
	var messages string
	for {
		line, err := reader.ReadString('\n')
		log.Println(line)
		if err != nil {
			if err == io.EOF {
				break
			}
			return "", fmt.Errorf("failed to read from file: %v", err)
		}
		messages += line
	}
	messages += "\n--END-OF-DATA--\n"
	return messages, nil
}
func handleConnection(conn net.Conn, r *Registry) {
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

		var protocolMsg Message
		err = json.Unmarshal([]byte(msg), &protocolMsg)
		if err != nil {
			log.Println("Error decoding message:", err)
			continue
		}
		switch protocolMsg.Type {
		case "register_producer":
			r.AddProducer(protocolMsg.ID, protocolMsg.Topic)
			conn.Write([]byte("Producer registered\n"))
		case "register_consumer":
			r.AddConsumer(protocolMsg.ID, protocolMsg.Group)
			log.Println("Consumer registered")
			conn.Write([]byte("Consumer registered\n"))
		case "produce":
			handleProduce(protocolMsg)
			conn.Write([]byte("Message produced\n"))
		case "consume":
			log.Println("Message sent")
			msg, err := handleConsume(protocolMsg.Topic, "0")
			if err!= nil {
                log.Println("Error consuming:", err)
                conn.Write([]byte("Error consuming\n"))
                continue
            }
			log.Println("Response: ",msg)
			conn.Write(append([]byte(msg), '\n'))
		default:
			log.Println("Unknown message type:", protocolMsg.Type)
			conn.Write([]byte("Unknown message type\n"))
		}

	}
}

func main() {
	r := NewRegistry()
	b := NewBroker(1, ":5000", r)
	b.Start()

}
