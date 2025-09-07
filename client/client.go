package main

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
)

var dataNodeList = make(map[string]string)

func splitFile(fileName string) [][]byte {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	buf := make([]byte, 1024)
	blocks := make([][]byte, 0)

	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		log.Fatal(err)
	}
	for {
		n, err := file.Read(buf)
		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			blocks = append(blocks, chunk)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
	}
	return blocks
}

func UploadFile(chunk []byte, blockID string, dataNode string, client *rpc.Client) {
	var address string
	_, exists := dataNodeList[dataNode]
	if !exists {
		err := client.Call("MasterNode.Discover", dataNode, &address)
		fmt.Println("DataNode on Host:", address, "err:", err)
		dataNodeList[dataNode] = address
	} else {
		address = dataNodeList[dataNode]
	}
	dnClient, err := net.Dial("tcp", address)
	if err != nil {
		log.Fatal("Dialing:", err)
	}
	defer dnClient.Close()
	checksum := md5.Sum(chunk)
	header := []byte("Upload\n")
	header = append(header, []byte("3\n")...)
	header = append(header, []byte(blockID+"\n")...)
	header = append(header, []byte(fmt.Sprintf("%x\n", checksum))...)
	packet := append(header, chunk...)
	_, err = dnClient.Write(packet)
	resp_buf := make([]byte, 25)
	n, err := dnClient.Read(resp_buf)
	fmt.Println("Response from Datanode ", string(resp_buf[:n]), "err:", err)
	if n == 0 {
		fmt.Println("Failed to Upload, please try again!")
	}
}

func DownloadFile(blockID string, offset int, length int, dataNode string, client *rpc.Client) {
	var address string
	_, exists := dataNodeList[dataNode]
	if !exists {
		err := client.Call("MasterNode.Discover", dataNode, &address)
		fmt.Println("DataNode on Host:", address, "err:", err)
		dataNodeList[dataNode] = address
	} else {
		address = dataNodeList[dataNode]
	}
	dnClient, err := net.Dial("tcp", address)
	if err != nil {
		log.Fatal("Dialing:", err)
	}
	defer dnClient.Close()
	header := []byte("Download\n")
	header = append(header, []byte(blockID+"\n")...)
	header = append(header, []byte(strconv.Itoa(offset)+"\n")...)
	packet := append(header, []byte(strconv.Itoa(length)+"\n")...)
	_, err = dnClient.Write(packet)
	if err != nil {
		log.Fatal("Error sending request!")
	}
	reader := bufio.NewReader(dnClient)
	checksumLine, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Error reading checksum:", err)
		return
	}
	checksumLine = checksumLine[:len(checksumLine)-1]

	// Now read the chunk bytes
	resp_buf := make([]byte, length)
	n, err := io.ReadFull(reader, resp_buf)
	if err != nil && err != io.EOF {
		fmt.Println("Error reading chunk:", err)
		return
	}

	fmt.Println("Checksum from Datanode:", checksumLine)
	fmt.Println("Received chunk bytes:", n)
	expectedCheckSum, err := hex.DecodeString(checksumLine)
	if err != nil {
		fmt.Println("Error decoding checksum:", err)
		return
	}
	calculatedCheckSum := md5.Sum(resp_buf)
	if !bytes.Equal(calculatedCheckSum[:], expectedCheckSum) {
		fmt.Println("Checksum mismatch!")
		return
	}
	fileName := blockID + ".txt"
	err = os.WriteFile(fileName, resp_buf[:n], 0644)
	if err != nil {
		fmt.Println("Error writing to file:", err)
		return
	}
}
//Currently doesn't take input from cmd. Specify functions in main func
func main() {
	client, err := rpc.Dial("tcp", "localhost:8080")
	if err != nil {
		log.Fatal("Dialing:", err)
	}

	var reply string
	var replySlice []string
	var request struct{}

	// Make directory "test_1"
	err = client.Call("MasterNode.MakeDirCommand", "test_1", &reply)
	fmt.Println("MakeDirCommand:", reply, "err:", err)

	// Change directory to "test_1/"
	err = client.Call("MasterNode.ChangeDirCommand", "test_1/", &reply)
	fmt.Println("ChangeDirCommand:", reply, "err:", err)

	// Create file in "test_1/"
	err = client.Call("MasterNode.CreateFileCommand", "test_file_1.txt 756 3 2048", &reply)
	fmt.Println("CreateFileCommand:", reply, "err:", err)

	// Make directory "test_4"
	err = client.Call("MasterNode.MakeDirCommand", "test_4", &reply)
	fmt.Println("MakeDirCommand:", reply, "err:", err)

	// Change directory to "test_4/"
	err = client.Call("MasterNode.ChangeDirCommand", "test_4/", &reply)
	fmt.Println("ChangeDirCommand:", reply, "err:", err)

	// Create file in "test_4/"
	err = client.Call("MasterNode.CreateFileCommand", "test_file_4.txt 756 3 4096", &reply)
	fmt.Println("CreateFileCommand:", reply, "err:", err)

	// List directories in current directory
	err = client.Call("MasterNode.ListDirCommand", request, &replySlice)
	fmt.Println("ListDirCommand:", replySlice, "err:", err)

	// List files in current directory
	err = client.Call("MasterNode.ListFilesCommand", request, &replySlice)
	fmt.Println("ListFilesCommand:", replySlice, "err:", err)

	//
	err = client.Call("MasterNode.GetFileInfoCommand", "test_file_4.txt", &reply)
	fmt.Println("ListFilesCommand:", reply, "err:", err)

	fileChunks := splitFile("test.txt")
	// for i := range fileChunks {
	UploadFile(fileChunks[0], "PQ7c7hWhFp", "TGiZw4zcAPBLeYTm", client)
	DownloadFile("PQ7c7hWhFp", 0, 1024, "TGiZw4zcAPBLeYTm", client)
	// }

}
