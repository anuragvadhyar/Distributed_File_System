package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
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

	_ "github.com/mattn/go-sqlite3"
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

func load_file_metadata_from_disk(wg *sync.WaitGroup, target map[string]FileMetadata, db *sql.DB) {
	defer wg.Done()
	rows, db_err := db.Query(`SELECT id, path, file_name, permissions, replication_factor, file_size FROM FILE_METADATA`)
	if db_err != nil {
		log.Fatal("Error accessing db!")
		return
	}
	defer rows.Close()

	for rows.Next() {
		var id, path, fileName, permissions string
		var replicationFactor, fileSize int
		if err := rows.Scan(&id, &path, &fileName, &permissions, &replicationFactor, &fileSize); err != nil {
			log.Fatal("Error scanning row:", err)
			return
		}
		// Compose the key as path+fileName (to match your map usage)
		blockRows, err := db.Query(`SELECT block_id, datanode_id FROM BLOCKS WHERE file_id = ?`, id)
		if err != nil {
			log.Fatal("Error querying blocks:", err)
			return
		}
		blocks := []BlockInfo{}
		for blockRows.Next() {
			var blockID, datanodeID string
			if err := blockRows.Scan(&blockID, &datanodeID); err != nil {
				log.Fatal("Error scanning block row:", err)
				return
			}
			blocks = append(blocks, BlockInfo{
				ID:         blockID,
				DataNodeID: datanodeID,
			})
		}
		blockRows.Close()

		key := path + fileName
		target[key] = FileMetadata{
			Permissions:       permissions,
			ReplicationFactor: replicationFactor,
			FileSize:          fileSize,
			Blocks:            blocks,
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
		return fmt.Errorf("invalid directory name")
	}
	if slices.Contains(children, path) {
		return fmt.Errorf("directory already exists on this level")
	}
	folderHierarchy[current_directory] = append(folderHierarchy[current_directory], path)
	folderHierarchy[current_directory+path+"/"] = []string{}
	res, err := db.Exec(`UPDATE FOLDERS SET directory = ? WHERE parent_path = ? AND directory IS NULL`, path, current_directory)
	rowsAffected, _ := res.RowsAffected()
	if err != nil || rowsAffected == 0 {
		// If no row was updated, insert as usual
		db.Exec(`INSERT INTO FOLDERS(parent_path, directory) VALUES (?, ?)`, current_directory, path)
	}

	// Always ensure the new directory exists as a parent (for leaf/empty dirs)
	db.Exec(`INSERT OR IGNORE INTO FOLDERS(parent_path, directory) VALUES (?, ?)`, current_directory+path+"/", nil)

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
			return fmt.Errorf("in root dir")
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
	fmt.Println(current_directory)
	fmt.Println(path)
	_, exists := folderHierarchy[(current_directory)+path]
	if !exists {
		return fmt.Errorf("directory doesn't exist")
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
		return fmt.Errorf("directory doesn't exist")
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
		return fmt.Errorf("filename must be provided")
	}
	file_name := fields[0]
	children := fileHierarchy[current_directory]
	isValidPath := is_valid_structure(&file_name)
	if !isValidPath {
		return fmt.Errorf("invalid file name")
	}
	if slices.Contains(children, file_name) {
		return fmt.Errorf("file already exists on this level")
	}
	fileHierarchy[current_directory] = append(fileHierarchy[current_directory], file_name)
	db.Exec(`INSERT INTO FILE_MAPPINGS(path, file_name) VALUES (?, ?)`, current_directory, file_name)
	if len(fields) > 2 {
		replicationFactor, err := strconv.Atoi(fields[2])
		if err != nil {
			return fmt.Errorf("invalid replication factor")
		}
		fileSize := 0
		if len(fields) > 3 {
			fileSize, err = strconv.Atoi(fields[3])
			if err != nil {
				return fmt.Errorf("invalid file size")
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

		res, err := db.Exec(`INSERT INTO FILE_METADATA(path, file_name, permissions, replication_factor, file_size) VALUES (?, ?, ?, ?, ?)`,
			current_directory, file_name, fields[1], replicationFactor, fileSize)
		if err != nil {
			return fmt.Errorf("failed to insert file metadata: %v", err)
		}
		fileID, err := res.LastInsertId()
		if err != nil {
			return fmt.Errorf("failed to get last insert id: %v", err)
		}
		// Insert each block with the retrieved file_id
		for _, block := range blocks {
			_, err := db.Exec(`INSERT INTO BLOCKS(block_id, file_id, datanode_id) VALUES (?, ?, ?)`,
				block.ID, fileID, block.DataNodeID)
			if err != nil {
				return fmt.Errorf("failed to insert block: %v", err)
			}
		}
		fileMetadataMap[current_directory+file_name] = m
	}
	*reply = strings.Join(block_Ids, " ")
	return nil
}

func (m *MasterNode) GetFileInfoCommand(path string, reply *string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(path) == 0 {
		return fmt.Errorf("path must be provided")
	}
	if rune(path[0]) == '/' {
		path = path[1:]
	}
	meta, exists := fileMetadataMap[current_directory+path]
	if !exists {
		return fmt.Errorf("file doesn't exist in given path %s", current_directory+path)
	}
	data, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %v", err)
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
		return fmt.Errorf("error Reaching Datanode %s", d_node)
	}
	*reply = "localhost:" + m.datanodes[d_node].tcp
	return nil
}

func load_from_disk(wg *sync.WaitGroup, table string, target map[string][]string, db *sql.DB) {
	defer wg.Done()
	switch table {
	case "FOLDERS":
		rows, db_err := db.Query(`SELECT parent_path, directory FROM FOLDERS`)
		if db_err != nil {
			log.Fatal("Error accessing db!")
			return
		}
		defer rows.Close()

		for rows.Next() {
			var parentPath string
			var directory sql.NullString
			if err := rows.Scan(&parentPath, &directory); err != nil {
				log.Fatal("Error scanning row:", err)
				return
			}
			if directory.Valid {
				target[parentPath] = append(target[parentPath], directory.String)
			} else {
				// If directory is NULL, ensure the key exists with an empty slice
				if _, exists := target[parentPath]; !exists {
					target[parentPath] = []string{}
				}
			}
		}

	case "FILE_MAPPINGS":
		rows, db_err := db.Query(`SELECT path, file_name FROM FILE_MAPPINGS`)
		if db_err != nil {
			log.Fatal("Error accessing db!")
			return
		}
		defer rows.Close()

		for rows.Next() {
			var parentPath, file_name string
			if err := rows.Scan(&parentPath, &file_name); err != nil {
				log.Fatal("Error scanning row:", err)
				return
			}
			target[parentPath] = append(target[parentPath], file_name)
		}
	}
}

var nameNode *MasterNode
var db *sql.DB

func init_namenode() {
	//add init for connections to client and mules
	var db_err error
	db, db_err = init_DB()
	if db_err != nil {
		log.Fatal(db_err)
	}
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
	go load_from_disk(nameNode.wg, "FOLDERS", folderHierarchy, db)
	go load_from_disk(nameNode.wg, "FILE_MAPPINGS", fileHierarchy, db)
	go load_file_metadata_from_disk(nameNode.wg, fileMetadataMap, db)
	nameNode.wg.Wait()
	go func() {
		rpc.Accept(nameNode.listener)
	}()
}

func destroy_namenode() {
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

func init_DB() (*sql.DB, error) {
	db, err := sql.Open("sqlite3", "DFS")
	if err != nil {
		return nil, err
	}
	// defer db.Close()
	// Create folders table
	_, err = db.Exec(`
        CREATE TABLE IF NOT EXISTS FOLDERS (
            parent_path TEXT,
            directory   TEXT,
			PRIMARY KEY (parent_path, directory)
        )
    `)
	if err != nil {
		return nil, err
	}

	// Create file_mappings table
	_, err = db.Exec(`
        CREATE TABLE IF NOT EXISTS FILE_MAPPINGS (
            path      TEXT,
            file_name TEXT,
			PRIMARY KEY (path, file_name)
        )
    `)
	if err != nil {
		return nil, err
	}

	// Create file_metadata table
	_, err = db.Exec(`
    CREATE TABLE IF NOT EXISTS FILE_METADATA (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
        path TEXT,
        file_name TEXT,
        permissions TEXT,
        replication_factor INTEGER,
        file_size INTEGER,
        UNIQUE (path, file_name)
    	)
	`)
	if err != nil {
		return nil, err
	}

	//create blocks table
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS BLOCKS (
			block_id TEXT PRIMARY KEY,
			file_id INTEGER,
			datanode_id TEXT,
			FOREIGN KEY (file_id) REFERENCES FILE_METADATA(id) ON DELETE CASCADE
		)
	`)
	if err != nil {
		return nil, err
	}
	return db, nil
}
func main() {
	fmt.Println("Hello, Welcome to Anurag Distributed File System!")
	init_namenode()
	fmt.Println(folderHierarchy)
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

//Add Polling algorithm to choose datanodes
