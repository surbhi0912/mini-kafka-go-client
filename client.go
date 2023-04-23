package main

import (
	"fmt"
	"log"
	"net"
	"os"
)

const (
	connHost = "localhost"
	connPort = "3333"
	connType = "tcp"
)

func main() {
	tcpServer, err := net.ResolveTCPAddr(connType, connHost+":"+connPort)
	if err != nil {
		// println("ResolveTCPAddr failed:", err.Error())
		log.Fatal(err)
		os.Exit(1)
	}

	conn, err := net.DialTCP(connType, nil, tcpServer)
	if err != nil {
		// println("Dial failed:", err.Error())
		log.Fatal(err)
		os.Exit(1)
	}

	// _, err = conn.Write([]byte("Hello world"))
	_, err = conn.Write([]byte("createTopic:lmno:partitions:3:replicas:2"))
	if err != nil {
		// println("Write data failed:", err.Error())
		log.Fatal(err)
		os.Exit(1)
	}
	conn.CloseWrite()

	// Making a buffer to hold incoming data
	received := make([]byte, 1024)
	// var received []byte
	// Read the incoming connection into the buffer.
	_, err = conn.Read(received)
	if err != nil {
		// println("Read data failed:", err.Error())
		log.Fatal(err)
		os.Exit(1)
	}


	fmt.Println("Received message from server - ", string(received))

	conn.Close()

	//########################################################

	tcpServer, err = net.ResolveTCPAddr(connType, connHost+":"+connPort)
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}

	conn, err = net.DialTCP(connType, nil, tcpServer)
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}

	_, err = conn.Write([]byte("produce:lmno:hi my name is surbhi"))
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
	conn.CloseWrite()

	received = make([]byte, 1024)
	_, err = conn.Read(received)
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}

	fmt.Println("Received message from server - ", string(received))

	conn.Close()

	//########################################################

	tcpServer, err = net.ResolveTCPAddr(connType, connHost+":"+connPort)
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}

	conn, err = net.DialTCP(connType, nil, tcpServer)
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}

	_, err = conn.Write([]byte("produce:lmno:I am a producer"))
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
	conn.CloseWrite()

	received = make([]byte, 1024)
	_, err = conn.Read(received)
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}

	fmt.Println("Received message from server - ", string(received))

	conn.Close()

	//########################################################

	tcpServer, err = net.ResolveTCPAddr(connType, connHost+":"+connPort)
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}

	conn, err = net.DialTCP(connType, nil, tcpServer)
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}

	_, err = conn.Write([]byte("consume:lmno"))
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
	conn.CloseWrite()

	// received = nil //clear the byte slice
	received = make([]byte, 1024)
	_, err = conn.Read(received)
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
	fmt.Println("Received message from server - ", string(received))

	conn.Close()
}