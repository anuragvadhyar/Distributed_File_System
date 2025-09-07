package main

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

type DataNode struct {
	listener   net.Listener
	id         string
	RPCaddress string
	TCPaddress string
}

type DataNodeAddress struct {
	RPC string
	TCP string
}

var datanodeIDMap = make(map[string]string)

func loadDataNodeIDs(filename string) {
	file, err := os.Open(filename)
	if err != nil {
		return
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	_ = decoder.Decode(&datanodeIDMap)
}

func saveDataNodeIDs(filename string) {
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Println("Failed to save datanode IDs:", err)
		return
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	_ = encoder.Encode(datanodeIDMap)
}

func generateRandomId(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	seededRand := rand.New(rand.NewSource(time.Now().UnixNano()))
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

var dataNode []*DataNode

func init_datanode(addresses []DataNodeAddress) {
	loadDataNodeIDs("datanode_id's.json")
	for _, addr := range addresses {
		id, ok := datanodeIDMap[addr.RPC]
		if !ok {
			id = generateRandomId(16)
			datanodeIDMap[addr.RPC] = id
		}
		dn := &DataNode{
			id:         id,
			RPCaddress: addr.RPC,
			TCPaddress: addr.TCP,
		}
		// RPC listener
		rpc.Register(dn)
		var err error
		dn.listener, err = net.Listen("tcp", ":"+dn.RPCaddress)
		if err != nil {
			log.Fatal(err)
			return
		}
		fmt.Printf("DataNode (RPC) is listening on port %s...\n", addr.RPC)
		go func(listener net.Listener) {
			rpc.Accept(listener)
		}(dn.listener)

		// TCP listener (for raw chunk transfer)
		go func(tcpAddr string) {
			ln, err := net.Listen("tcp", ":"+tcpAddr)
			if err != nil {
				log.Fatal("TCP Listen error:", err)
			}
			fmt.Printf("DataNode (TCP) is listening on port %s...\n", tcpAddr)
			for {
				conn, err := ln.Accept()
				if err != nil {
					fmt.Println("TCP Accept error:", err)
					continue
				}
				go handleTCPChunk(conn, dn.id)
			}
		}(dn.TCPaddress)

		go func(id, rpcAddr, tcpAddr string) {
			pingMaster(id, rpcAddr, tcpAddr)
		}(dn.id, dn.RPCaddress, dn.TCPaddress)
		dataNode = append(dataNode, dn)
	}
}

func handleUpload(reader *bufio.Reader, conn net.Conn, id string) {
	replicationCount, err := reader.ReadString('\n')
	replicationCount = replicationCount[:len(replicationCount)-1]
	if err != nil {
		fmt.Println("Error reading block ID:", err)
		return
	}
	blockID, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Error reading block ID:", err)
		return
	}
	blockID = blockID[:len(blockID)-1]
	checkSum, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Error reading Checksum:", err)
		return
	}
	checkSum = checkSum[:len(checkSum)-1]
	fmt.Printf("Checksum received %s\n", checkSum)
	chunk := make([]byte, 1024)
	n, err := reader.Read(chunk)
	if err != nil && err != io.EOF {
		fmt.Println("Error reading chunk:", err)
		return
	}
	fmt.Printf("Received block ID: %s\n", blockID)
	expectedCheckSum, err := hex.DecodeString(checkSum)
	if err != nil {
		fmt.Println("Error decoding checksum:", err)
		return
	}
	calculatedCheckSum := md5.Sum(chunk[:n])
	if !bytes.Equal(calculatedCheckSum[:], expectedCheckSum) {
		fmt.Println("Checksum mismatch!")
		return
	}

	fileName := blockID + ".txt"
	uniqueFileName := fileName
	suffix := 1
	for {
		if _, err := os.Stat(uniqueFileName); os.IsNotExist(err) {
			break
		}
		uniqueFileName = fmt.Sprintf("%s_%s.txt", blockID, id)
		suffix++
	}

	err = os.WriteFile(uniqueFileName, chunk[:n], 0644)
	if err != nil {
		fmt.Println("Error writing chunk to file:", err)
		return
	}
	var myIndex int = -1
	for i := range dataNode {
		if dataNode[i].id == id {
			myIndex = i
			break
		}
	}
	replicationFactor, err := strconv.Atoi(replicationCount)
	if err != nil {
		fmt.Println("Enter valid replication factor")
	}
	err = forward_replica(replicationFactor, myIndex, blockID, chunk[:n], checkSum)
	if err != nil {
		// Replication failed: delete all files with blockID prefix
		files, _ := os.ReadDir(".")
		for _, f := range files {
			if !f.IsDir() && len(f.Name()) >= len(blockID) && f.Name()[:len(blockID)] == blockID {
				os.Remove(f.Name())
			}
		}
		// conn.Write([]byte("failed, please retry\n"))
		return
	}
	conn.Write([]byte("received successfully\n"))
}

func handleDownload(reader *bufio.Reader, conn net.Conn) {
	blockID, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Error reading block ID:", err)
		return
	}
	blockID = blockID[:len(blockID)-1]
	offset, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Error reading Offset:", err)
		return
	}
	offset = offset[:len(offset)-1]
	fmt.Printf("Offset is %s:\n", offset)
	length, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Error reading block ID:", err)
		return
	}
	length = length[:len(length)-1]
	fmt.Printf("Length is %s:\n", length)
	chunkLength, err := strconv.Atoi(length)
	if err != nil {
		fmt.Println("Invalid length")
		return
	}
	chunkOffset, err := strconv.Atoi(offset)
	if err != nil {
		fmt.Println("Invalid length")
		return
	}
	chunk := make([]byte, chunkLength)
	var fileName string
	files, err := os.ReadDir(".")
	if err != nil {
		fmt.Println("Error reading directory:", err)
		return
	}
	found := false
	for _, f := range files {
		if !f.IsDir() && len(f.Name()) >= len(blockID) && f.Name()[:len(blockID)] == blockID {
			fileName = f.Name()
			found = true
			break
		}
	}
	if !found {
		fmt.Printf("No file found with prefix %s\n", blockID)
		return
	}

	file, err := os.OpenFile(fileName, os.O_RDWR, 0644)
	if err != nil {
		log.Fatal(err)
		return
	}
	defer file.Close()
	_, err = file.Seek(int64(chunkOffset), 0)
	if err != nil {
		log.Fatal(err)
		return
	}
	n, err := file.Read(chunk)
	if err != nil && err != io.EOF {
		fmt.Println("Error reading file chunk:", err)
		return
	}
	calculatedCheckSum := md5.Sum(chunk[:n])
	checksumStr := fmt.Sprintf("%x\n", calculatedCheckSum)
	response := append([]byte(checksumStr), chunk[:n]...)
	_, err = conn.Write(response)
	if err != nil {
		fmt.Println("Error sending chunk to client:", err)
		return
	}

}

func handleTCPChunk(conn net.Conn, id string) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	req, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Error reading request:", err)
		return
	}
	req = req[:len(req)-1]
	if req == "Upload" {
		handleUpload(reader, conn, id)
	} else {
		handleDownload(reader, conn)
	}
}

func destroy_datanode() {
	for _, dn := range dataNode {
		if dn.listener != nil {
			err := dn.listener.Close()
			if err != nil {
				log.Fatal(err)
				return
			}
		}
	}
	saveDataNodeIDs("datanode_id's.json")
}

func pingMaster(datanodeID, rpcAddress, tcpAddress string) {
	for {
		client, err := rpc.Dial("tcp", "localhost:8080")
		if err != nil {
			fmt.Println("Failed to connect to namenode:", err)
			time.Sleep(5 * time.Second)
			continue
		}
		var reply string
		args := [3]string{datanodeID, rpcAddress, tcpAddress}
		err = client.Call("MasterNode.Ping", args, &reply)
		if err != nil {
			fmt.Println("Ping to namenode failed:", err)
		} else {
			fmt.Println("Reply from namenode:", reply)
		}
		time.Sleep(10 * time.Second)
	}
}

func forward_replica(n int, index int, blockID string, chunk []byte, checksum string) error {
	fmt.Println("In forward")
	if n == 0 || len(dataNode) < 2 {
		return nil
	}
	nextIndex := (index + 1) % len(dataNode)
	if nextIndex == index {
		return nil
	}
	target := dataNode[nextIndex]
	address := "localhost:" + target.TCPaddress

	conn, err := net.Dial("tcp", address)
	if err != nil {
		fmt.Printf("Failed to forward to datanode %s: %v\n", target.id, err)
		return fmt.Errorf("Failed to forward to datanode %s: %v\n", target.id, err)
	}
	defer conn.Close()

	header := []byte("Upload\n")
	n -= 1
	replication := strconv.Itoa(n)
	header = append(header, []byte(replication+"\n")...)
	header = append(header, []byte(blockID+"\n")...)
	header = append(header, []byte(checksum+"\n")...)
	packet := append(header, chunk...)
	_, err = conn.Write(packet)
	if err != nil {
		fmt.Printf("Error forwarding chunk to datanode %s: %v\n", target.id, err)
		return fmt.Errorf("Error forwarding chunk to datanode %s: %v\n", target.id, err)
	}
	// Recursively forward to the next datanode
	return nil
}

func main() {
	fmt.Println("Hello, Welcome to Anurag Distributed File System!")
	init_datanode([]DataNodeAddress{
		{RPC: "1234", TCP: "5678"},
		{RPC: "8888", TCP: "7777"},
	})
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c // Block until signal received
	destroy_datanode()
}
