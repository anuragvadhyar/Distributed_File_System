package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode"
)

type BlockInfo struct {
	ID         string `json:"block_id"`
	DataNodeID string `json:"datanode_id"`
}

type Addresses struct {
	rpc string
	tcp string
}
type MasterNode struct {
	listener        net.Listener
	wg              *sync.WaitGroup
	mu              *sync.Mutex
	datanode_status map[string]time.Time
	datanodes       map[string]Addresses
}

type FileMetadata struct {
	Permissions       string      `json:"permissions"`
	ReplicationFactor int         `json:"replication_factor"`
	FileSize          int         `json:"file_size"`
	Blocks            []BlockInfo `json:"blocks"`
}

var current_directory string = "/"
var folderHierarchy = make(map[string][]string)
var fileHierarchy = make(map[string][]string)
var fileMetadataMap = make(map[string]FileMetadata)

const blockSize int = 1024

func generateRandomId(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	seededRand := rand.New(rand.NewSource(time.Now().UnixNano()))
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

func load_into_map(fileName *os.File, target map[string][]string) error {
	err := json.NewDecoder(fileName).Decode(&target)
	if err != nil {
		return err
	}
	return nil
}

func store_in_disk(fileName *os.File, target map[string][]string) error {
	err := json.NewEncoder(fileName).Encode(&target)
	if err != nil {
		return err
	}
	return nil
}

func load_file_metadata_from_disk(wg *sync.WaitGroup, file_name string) {
	defer wg.Done()
	file, err := os.OpenFile(file_name, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Fatal(err)
		return
	}
	defer file.Close()
	file_length, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		log.Fatal(err)
		return
	}
	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		log.Fatal(err)
		return
	}
	if file_length != 0 {
		decoder := json.NewDecoder(file)
		err = decoder.Decode(&fileMetadataMap)
		if err != nil {
			log.Fatal(err)
			return
		}
	}
}

func is_valid_structure(path *string) bool {
	for len(*path) > 0 && []rune(*path)[0] == '/' {
		*path = (*path)[1:]
	}
	if len(*path) == 0 {
		return false
	}
	if strings.ContainsRune(*path, ' ') {
		return false
	}
	runes := []rune(*path)
	if len(runes) == 0 || !unicode.IsLetter(runes[0]) {
		return false
	}
	return true
}

func (m *MasterNode) MakeDirCommand(path string, reply *string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	children := folderHierarchy[current_directory]
	isValidPath := is_valid_structure(&path)
	if !isValidPath {
		return fmt.Errorf("Invalid directory name!")
	}
	if slices.Contains(children, path) {
		return fmt.Errorf("Directory already exists on this level")
	}
	folderHierarchy[current_directory] = append(folderHierarchy[current_directory], path)
	folderHierarchy[current_directory+path+"/"] = []string{}
	*reply = "Directory created successfully"
	return nil
}

func (m *MasterNode) ListDirCommand(request struct{}, reply *[]string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	*reply = folderHierarchy[current_directory]
	return nil
}

func (m *MasterNode) ListFilesCommand(request struct{}, reply *[]string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	*reply = fileHierarchy[current_directory]
	return nil
}

func (m *MasterNode) ChangeDirCommand(path string, reply *string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if path == ".." {
		if current_directory == "/" {
			return fmt.Errorf("In root dir!")
		}
		dir := current_directory
		if strings.HasSuffix(dir, "/") && len(dir) > 1 {
			dir = dir[:len(dir)-1]
		}
		lastIndex := strings.LastIndex(dir, "/")
		if lastIndex > 0 {
			dir = dir[:lastIndex+1]
		} else {
			dir = "/"
		}
		current_directory = dir
		return nil
	}
	_, exists := folderHierarchy[(current_directory)+path]
	if !exists {
		return fmt.Errorf("Directory doesn't exist")
	}
	current_directory = (current_directory) + path
	*reply = "Directory Changed Successfully"
	return nil
}

func (m *MasterNode) RemoveCommand(path string, reply *string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	index := slices.Index(folderHierarchy[current_directory], path)
	if index == -1 {
		return fmt.Errorf("Directory doesn't exist")
	}
	folderHierarchy[current_directory] = append(folderHierarchy[current_directory][:index],
		folderHierarchy[current_directory][index+1:]...)
	fmt.Printf("to delete %s\n", current_directory+path+"/")
	delete(folderHierarchy, current_directory+path+"/")
	*reply = "Directory Removed Successfully!"
	return nil
}

// input order: filename, permissions, replication_factor, block_size, file_size
func (m *MasterNode) CreateFileCommand(args string, reply *string) error {
	block_Ids := make([]string, 0)
	m.mu.Lock()
	defer m.mu.Unlock()
	fields := strings.Fields(args)
	if len(fields) < 1 {
		return fmt.Errorf("Filename must be provided!")
	}
	file_name := fields[0]
	children := fileHierarchy[current_directory]
	isValidPath := is_valid_structure(&file_name)
	if !isValidPath {
		return fmt.Errorf("Invalid file name!")
	}
	if slices.Contains(children, file_name) {
		return fmt.Errorf("File already exists on this level")
	}
	fileHierarchy[current_directory] = append(fileHierarchy[current_directory], file_name)
	if len(fields) > 2 {
		replicationFactor, err := strconv.Atoi(fields[2])
		if err != nil {
			return fmt.Errorf("Invalid replication factor")
		}
		fileSize := 0
		if len(fields) > 3 {
			fileSize, err = strconv.Atoi(fields[3])
			if err != nil {
				return fmt.Errorf("Invalid file size")
			}
		}
		blocks := make([]BlockInfo, 0)
		numBlocks := (fileSize + blockSize - 1) / blockSize
		for i := 0; i < int(numBlocks); i++ {
			chunk := BlockInfo{
				DataNodeID: m.pollDataNodes(),
				ID:         generateRandomId(10),
			}
			blocks = append(blocks, chunk)
			block_Ids = append(block_Ids, chunk.ID)
		}
		m := FileMetadata{
			Permissions:       fields[1],
			ReplicationFactor: replicationFactor,
			FileSize:          fileSize,
			Blocks:            blocks,
		}
		file, err := os.OpenFile("file_metadata.json", os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return fmt.Errorf("%v", err)
		}
		defer file.Close()
		file_length, err := file.Seek(0, io.SeekEnd)
		if err != nil {
			return fmt.Errorf("%v", err)
		}
		_, err = file.Seek(0, io.SeekStart)
		if err != nil {
			return fmt.Errorf("%v", err)
		}
		if file_length != 0 {
			decoder := json.NewDecoder(file)
			err = decoder.Decode(&fileMetadataMap)
			if err != nil {
				return fmt.Errorf("%v", err)
			}
		}
		fileMetadataMap[current_directory+file_name] = m
		file.Truncate(0)
		file.Seek(0, 0)
		encoder := json.NewEncoder(file)
		err = encoder.Encode(fileMetadataMap)
		if err != nil {
			return fmt.Errorf("%v", err)
		}
	}
	*reply = strings.Join(block_Ids, " ")
	return nil
}

func (m *MasterNode) GetFileInfoCommand(path string, reply *string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(path) == 0 {
		return fmt.Errorf("Path must be provided!")
	}
	if rune(path[0]) == '/' {
		path = path[1:]
	}
	meta, exists := fileMetadataMap[current_directory+path]
	if !exists {
		return fmt.Errorf("File doesn't exist in given path %s!\n", current_directory+path)
	}
	data, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("Failed to marshal metadata: %v", err)
	}
	*reply = string(data)
	return nil
}

func (m *MasterNode) Ping(args [3]string, reply *string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := args[0]
	address := args[1]
	tcp_address := args[2]
	_, exists := m.datanodes[id]
	if !exists {
		m.datanodes[id] = Addresses{rpc: address, tcp: tcp_address}
	}
	m.datanode_status[id] = time.Now()
	fmt.Printf("DataNode %s address %s pinged. Adding to list!\n", id, address)
	*reply = fmt.Sprintf("Namenode received ping from datanode %s", id)
	return nil
}

func (m *MasterNode) Discover(d_node string, reply *string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, exists := m.datanodes[d_node]
	if !exists {
		return fmt.Errorf("Error Reaching Datanode %s", d_node)
	}
	*reply = "localhost:" + m.datanodes[d_node].tcp
	return nil
}

func load_from_disk(wg *sync.WaitGroup, file_name string, target map[string][]string) {
	defer wg.Done()
	file, err := os.OpenFile(file_name, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Fatal(err)
		return
	}
	file_length, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		log.Fatal(err)
		return
	}
	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		log.Fatal(err)
		return
	}
	if file_length != 0 {
		err = load_into_map(file, target)
		if err != nil {
			log.Fatal(err)
			return
		}
	}
	file.Close()
}

func save_to_disk(wg *sync.WaitGroup, file_name string, target map[string][]string) {
	defer wg.Done()
	file, err := os.OpenFile(file_name, os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatal(err)
		return
	}
	store_in_disk(file, target)
	file.Close()
}

var nameNode *MasterNode

func init_namenode() {
	nameNode = &MasterNode{
		wg:              new(sync.WaitGroup),
		mu:              &sync.Mutex{},
		datanode_status: make(map[string]time.Time),
		datanodes:       make(map[string]Addresses),
	}
	rpc.Register(nameNode)
	var err error
	nameNode.listener, err = net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatal(err)
		return
	}
	fmt.Println("NameNode is listening on port 8080...")
	nameNode.wg.Add(3)
	go load_from_disk(nameNode.wg, "folder_directory.json", folderHierarchy)
	go load_from_disk(nameNode.wg, "file_mappings.json", fileHierarchy)
	go load_file_metadata_from_disk(nameNode.wg, "file_metadata.json")
	nameNode.wg.Wait()
	go func() {
		rpc.Accept(nameNode.listener)
	}()
}

func destroy_namenode() {
	nameNode.wg.Add(2)
	go save_to_disk(nameNode.wg, "folder_directory.json", folderHierarchy)
	go save_to_disk(nameNode.wg, "file_mappings.json", fileHierarchy)
	nameNode.wg.Wait()

	if nameNode.listener != nil {
		err := nameNode.listener.Close()
		if err != nil {
			log.Fatal(err)
			return
		}
	}
}

func (m *MasterNode) check_datanode() {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	for id, lastPing := range m.datanode_status {
		if now.Sub(lastPing) > 10*time.Second {
			fmt.Printf("deleting dataNode %s from list\n", id)
			delete(m.datanode_status, id)
			delete(m.datanodes, id)
		}
	}
}

func (m *MasterNode) pollDataNodes() string {
	keys := make([]string, 0, len(nameNode.datanodes))
	for k := range nameNode.datanodes {
		keys = append(keys, k)
	}
	if len(keys) == 0 {
		return ""
	}
	randomIndex := rand.Intn(len(keys))
	return keys[randomIndex]
}

func main() {
	fmt.Println("Hello, Welcome to Anurag Distributed File System!")
	init_namenode()
	go func() {
		for {
			time.Sleep(5 * time.Second)
			nameNode.check_datanode()
		}
	}()
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c // Block until signal received
	destroy_namenode()
}
