package main

import (
	"fmt"
	"github.com/joho/godotenv"
	"github.com/reza-mirjahanian/l2-data-indexer/server/goEth"
	"github.com/reza-mirjahanian/l2-data-indexer/server/util"
	"os"
	"strconv"
	"strings"
)

// Config holds all application configuration parameters
type Config struct {
	DbFilePath           string
	SmartContractAddress string
	EventTopicFilter     string
	StartBlockNumber     int64
	BlockBatchSize       int64
	NodesRPC             []string
}

func main() {
	fmt.Println(util.BrightCyan + "Starting server ... " + util.Reset)
	config := loadConfiguration()
	newClient := createAndStartClient(config)
	defer newClient.Close()
	newClient.StartDataConsumption()
}

// loadConfiguration loads and validates all configuration parameters
func loadConfiguration() *Config {
	if err := godotenv.Load(); err != nil {
		panic(fmt.Sprintln(util.BrightRed+"loadConfiguration(): Error loading .env file"+util.Reset, err))
	}

	return &Config{
		DbFilePath:           getEnvOrExit("DB_FILE_PATH"),
		SmartContractAddress: getEnvOrExit("SMART_CONTRACT_ADDRESS"),
		EventTopicFilter:     getEnvOrExit("EVENT_TOPIC_FILTER"),
		StartBlockNumber:     parseIntOrExit(getEnvOrExit("START_BLOCK_NUMBER")),
		BlockBatchSize:       parseIntOrExit(getEnvOrExit("BLOCK_BATCH_SIZE")),
		NodesRPC:             parseNodeList(getEnvOrExit("NODES_RPC")),
	}
}

// getEnvOrExit retrieves an environment variable or exits if not found
func getEnvOrExit(key string) string {
	value := os.Getenv(key)
	if value == "" {
		panic(fmt.Sprintln(util.BrightRed+"getEnvOrExit() Key not set in .Env file "+util.Reset, key))
	}
	return value
}

// parseIntOrExit converts a string to int64 or exits on failure
func parseIntOrExit(value string) int64 {
	result, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		panic(fmt.Sprintln(util.BrightRed+"parseIntOrExit() convert to int64 "+util.Reset, value, err))
	}
	return result
}

func parseNodeList(path string) []string {
	if path == "" {
		return []string{}
	}

	nodes := strings.Split(path, ";")
	var result []string

	for _, node := range nodes {
		// Trim spaces and filter out empty entries
		node = strings.TrimSpace(node)
		if node != "" {
			result = append(result, node)
		}
	}
	if len(result) == 0 {
		panic(fmt.Sprintln(util.BrightRed+"parseNodeList() empty NodeList "+util.Reset, path))
	}

	return result

}

// createAndStartClient initializes and returns the workers instance
func createAndStartClient(config *Config) *goEth.DataConsumerClient {
	logConfiguration(config)

	c := goEth.NewDataConsumerClient(
		config.DbFilePath,
		config.NodesRPC,
		config.SmartContractAddress,
		config.EventTopicFilter,
		config.StartBlockNumber,
		config.BlockBatchSize,
	)

	return c
}

// logConfiguration prints the important configuration parameters
func logConfiguration(config *Config) {
	fmt.Println(util.BrightCyan+"DB_FILE_PATH: "+util.Reset, config.DbFilePath)
	fmt.Println(util.BrightCyan+"EVENT_TOPIC_FILTER: "+util.Reset, config.EventTopicFilter)
	fmt.Println(util.BrightCyan+"SMART_CONTRACT_ADDRESS: "+util.Reset, config.SmartContractAddress)
	fmt.Println(util.BrightCyan+"NodesRPC: "+util.Reset, config.NodesRPC)
	fmt.Println(util.BrightCyan+"StartBlockNumber: "+util.Reset, config.StartBlockNumber)
	fmt.Println(util.BrightCyan+"BlockBatchSize: "+util.Reset, config.BlockBatchSize)
}
