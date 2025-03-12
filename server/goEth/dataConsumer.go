package goEth

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/reza-mirjahanian/l2-data-indexer/server/db"
)

const (
	contractBucketName = "contract_handling" // Bucket for contract related data
	logsBucketName     = "keyed_logs"        // Bucket to store keyed logs
	lastBlockKeyName   = "last_block"        // Key for the last processed block number
	currentIndexKey    = "next_index"        // Key for the next available index
)

// DataConsumerClient defines the structure for consuming and storing blockchain data.
type DataConsumerClient struct {
	database   *db.BoltDBWrapper          // Database instance
	ethNodes   map[string]*EthereumClient // Map of Ethereum clients
	startBlock int64                      // Initial block to start indexing from
	stepSize   int64                      // Number of blocks to process in each step
	dbMutex    sync.Mutex                 // Mutex to protect database operations
}

type KeyedLog struct {
	RootData   string `json:"rootData"`
	ParentHash string `json:"parentHash"`
	BlockTime  uint64 `json:"blockTime"`
}

// NewDataConsumerClient initializes a new data consumer workers.
func NewDataConsumerClient(
	dbPath string,
	rpcURLs []string,
	contractAddr string,
	filterTopic string,
	initBlock int64,
	blockSize int64,
) *DataConsumerClient {
	boltDB, err := db.NewBoltDB(db.DBConfig{
		Path: dbPath,
	})
	if err != nil {
		log.Fatalf("Failed to initialize BoltDB: %v", err) // More descriptive log
	}

	ethereumClients := make(map[string]*EthereumClient)
	for _, url := range rpcURLs {
		ethClient, clientErr := NewEthereumClient(
			url,
			contractAddr,
			"abi/sequencer.json",
			filterTopic,
		)
		if clientErr != nil {
			log.Fatalf("Error creating Ethereum workers for %s: %v", url, clientErr) // Improved error message
		}
		ethereumClients[url] = ethClient
	}

	return &DataConsumerClient{
		database:   boltDB,
		ethNodes:   ethereumClients,
		startBlock: initBlock,
		stepSize:   blockSize,
		dbMutex:    sync.Mutex{}, // Explicit initialization (though default is also fine)
	}
}

// fetchStartingBlock retrieves the block number to start processing from.
// It checks the database for the last processed block; otherwise, it uses the initial block.
func (dc *DataConsumerClient) fetchStartingBlock() int64 {

	blockValue, readErr := dc.database.ReadFromDB(contractBucketName, lastBlockKeyName)

	if readErr != nil {
		log.Fatalf("Database read error for %s: %v", lastBlockKeyName, readErr) // More context in log
	}

	if blockValue == "" {
		log.Printf("No previous block found. Starting from configured initial block: %d", dc.startBlock)
		return dc.startBlock
	}

	lastBlock, parseErr := strconv.ParseInt(blockValue, 10, 64)
	if parseErr != nil {
		log.Fatalf("Failed to parse last block number: %v", parseErr) // Specific error message
	}
	return lastBlock
}

// StartDataConsumption initiates the data processing service.
// It sets up workers for each Ethereum workers and distributes block ranges for processing.
func (dc *DataConsumerClient) StartDataConsumption() {
	blockRangeChan := make(chan struct{ from, to int64 })
	initialBlock := dc.fetchStartingBlock()

	// Launch worker goroutines for each Ethereum node
	for nodeEndpoint, ethNodeClient := range dc.ethNodes {
		go func(endpoint string, client *EthereumClient, ranges <-chan struct{ from, to int64 }) {
			dc.processNodeClient(endpoint, client, ranges)
		}(nodeEndpoint, ethNodeClient, blockRangeChan)
	}

	// Dispatch block ranges to workers in a separate goroutine
	go func() {
		currentBlock := initialBlock
		for {
			endBlock := currentBlock + dc.stepSize
			blockRangeChan <- struct{ from, to int64 }{from: currentBlock, to: endBlock}
			currentBlock = endBlock
			time.Sleep(5 * time.Second) // Wait time between block range distribution
		}
	}()

	select {} // Keep the main goroutine alive indefinitely
}

// processNodeClient is the worker function responsible for fetching and storing logs for a given workers.
func (dc *DataConsumerClient) processNodeClient(nodeEndpoint string, ethClient *EthereumClient, blockRanges <-chan struct{ from, to int64 }) {
	for rangeInfo := range blockRanges {
		startBlock, finishBlock := rangeInfo.from, rangeInfo.to

		fetchedLogs, err := ethClient.KeyedLogs(startBlock, finishBlock)
		if err != nil {
			log.Fatalf("[%s] Log retrieval failed: %v", nodeEndpoint, err) // More concise log message
		}

		endpointSuffix := nodeEndpoint[len(nodeEndpoint)-4:] // Get last 4 chars of endpoint for logging
		log.Printf("[%s] Processing blocks: %d to %d", endpointSuffix, startBlock, finishBlock)
		log.Printf("[%s] Number of logs fetched: %d", endpointSuffix, len(fetchedLogs))

		dc.dbMutex.Lock() // Acquire lock before database operations

		// Retrieve the next index for log entries
		indexValue, err := dc.database.ReadFromDB(contractBucketName, currentIndexKey)
		if err != nil {
			log.Fatalf("[%s] Failed to read %s from DB: %v", nodeEndpoint, currentIndexKey, err)
		}

		var nextLogIndex int64
		if indexValue == "" {
			nextLogIndex = 0
		} else {
			nextLogIndex, err = strconv.ParseInt(indexValue, 10, 64)
			if err != nil {
				log.Fatalf("[%s] Error parsing %s: %v", nodeEndpoint, currentIndexKey, err)
			}
		}

		// Persist each log entry into the database
		for logIndexOffset, currentLog := range fetchedLogs {
			recordIndex := strconv.FormatInt(nextLogIndex+int64(logIndexOffset), 10)
			logJSON, marshalErr := json.Marshal(currentLog)
			if marshalErr != nil {
				log.Fatalf("[%s] JSON encoding error: %v", nodeEndpoint, marshalErr)
			}
			if storeErr := dc.database.WriteToDB(logsBucketName, recordIndex, string(logJSON)); storeErr != nil {
				log.Fatalf("[%s] DB write error: %v", nodeEndpoint, storeErr)
			}
		}

		// Update next index and last processed block
		updatedIndex := nextLogIndex + int64(len(fetchedLogs))
		if updateIndexErr := dc.database.WriteToDB(contractBucketName, currentIndexKey, fmt.Sprintf("%d", updatedIndex)); updateIndexErr != nil {
			log.Fatalf("[%s] Failed to update %s in DB: %v", nodeEndpoint, currentIndexKey, updateIndexErr)
		}
		if updateBlockErr := dc.database.WriteToDB(contractBucketName, lastBlockKeyName, fmt.Sprintf("%d", finishBlock)); updateBlockErr != nil {
			log.Fatalf("[%s] Failed to update %s in DB: %v", nodeEndpoint, lastBlockKeyName, updateBlockErr)
		}

		dc.dbMutex.Unlock() // Release lock after database operations
	}
}

// DisplayIndexedLogs retrieves and prints the stored keyed logs from the database.
func (dc *DataConsumerClient) DisplayIndexedLogs() {
	indexValue, err := dc.database.ReadFromDB(contractBucketName, currentIndexKey)
	if err != nil {
		log.Fatalf("Failed to read %s from database: %v", currentIndexKey, err)
	}
	if indexValue == "" {
		log.Println("No logs indexed yet.") // Informative message when no logs are present
		return
	}

	lastLogIndex, err := strconv.ParseInt(indexValue, 10, 64)
	if err != nil {
		log.Fatalf("Error converting %s to integer: %v", currentIndexKey, err)
	}

	for logIndex := int64(0); logIndex < lastLogIndex; logIndex++ {
		logEntryValue, readErr := dc.database.ReadFromDB(logsBucketName, strconv.FormatInt(logIndex, 10))
		if readErr != nil {
			log.Fatalf("Database read error: %v", readErr)
		}

		if logEntryValue == "" {
			log.Printf("Index:%d, Warning: No data found in DB.", logIndex) // More user-friendly warning
			continue
		}

		var logData KeyedLog
		if unmarshalErr := json.Unmarshal([]byte(logEntryValue), &logData); unmarshalErr != nil {
			log.Printf("Index:%d, JSON decode error: %v", logIndex, unmarshalErr) // Simpler error message
		} else {
			log.Printf(
				"\tIndex: %d,\n\tRoot Data: %s,\n\tParent Hash: %s,\n\tBlock Timestamp: %d", // Slightly altered output labels
				logIndex,
				logData.RootData,
				logData.ParentHash,
				logData.BlockTime,
			)
		}
	}
}

// Close gracefully shuts down the data consumer workers, closing the database connection.
func (dc *DataConsumerClient) Close() {
	dc.database.Close()
	log.Println("Database connection closed.") // Confirmation log for shutdown
}
