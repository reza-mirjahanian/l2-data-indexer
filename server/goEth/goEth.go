package goEth

import (
	"context"
	"fmt"
	"log"
	"math/big"
	"os"
	"strings"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
)

// EthereumClient wraps the functionality to interact with an Ethereum contract
type EthereumClient struct {
	client          *ethclient.Client
	contractABI     *abi.ABI
	contractAddress common.Address
	topicToFilter   string
}

// NewEthereumClient creates a new EthereumClient instance
func NewEthereumClient(
	providerURL,
	contractAddress,
	contractABIPath string,
	topicToFilter string,
) (*EthereumClient, error) {
	// Connect to the Ethereum workers
	client, err := ethclient.Dial(providerURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to the Ethereum workers: %w", err)
	}

	// Parse the ABI
	abiFileContent, err := os.ReadFile(contractABIPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read contract ABI file: %w", err)
	}

	parsedABI, err := abi.JSON(strings.NewReader(string(abiFileContent)))
	if err != nil {
		return nil, fmt.Errorf("failed to parse contract ABI: %w", err)
	}

	// Define the contract address
	contractAddr := common.HexToAddress(contractAddress)

	return &EthereumClient{
		client:          client,
		contractABI:     &parsedABI,
		contractAddress: contractAddr,
		topicToFilter:   topicToFilter,
	}, nil
}

// FetchLogs retrieves and processes logs from the contract
func (ec *EthereumClient) FetchLogs(from, to int64) error {
	log.Println("Fetching logs...")
	// Create a filter query
	query := ethereum.FilterQuery{
		Addresses: []common.Address{ec.contractAddress},
		Topics:    [][]common.Hash{},
		FromBlock: big.NewInt(from),
		ToBlock:   big.NewInt(to),
	}

	// Fetch logs
	logs, err := ec.client.FilterLogs(context.Background(), query)
	if err != nil {
		return fmt.Errorf("failed to retrieve logs: %w", err)
	}

	log.Printf("Fetched %d logs\n", len(logs))

	// counter := 0

	// Process and print logs
	for _, vLog := range logs {

		log.Printf(
			"\tLog Block Number: %d\n\tLog Index: %d\n\tLog Address: %s\n\tLog Data: %s\n\tLog Topics: %v\n",
			vLog.BlockNumber,
			vLog.Index,
			vLog.Address.Hex(),
			common.Bytes2Hex(vLog.Data),
			vLog.Topics,
		)

		for i, topic := range vLog.Topics {
			if topic.Hex() == ec.topicToFilter {
				log.Println("Matched topic at", i, "!")
			}
		}

		// Decode log data if needed
		event := struct {
			Value *big.Int
		}{}
		err := ec.contractABI.UnpackIntoInterface(&event, "ValueChanged", vLog.Data)
		if err != nil {
			log.Printf("Failed to unpack log data: %v", err)
		} else {
			fmt.Printf("Event Value: %s\n", event.Value.String())
		}
	}

	log.Println("End of fetching logs...")

	return nil
}

// Create util.KeyedLog instances from the logs
func (ec *EthereumClient) KeyedLogs(from, to int64) ([]KeyedLog, error) {

	// Create a filter query
	query := ethereum.FilterQuery{
		Addresses: []common.Address{ec.contractAddress},
		Topics:    [][]common.Hash{},
		FromBlock: big.NewInt(from),
		ToBlock:   big.NewInt(to),
	}

	// Fetch logs
	logs, err := ec.client.FilterLogs(context.Background(), query)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve logs: %w", err)
	}

	keyedLogs := make([]KeyedLog, 0, len(logs))

	for _, log := range logs {

		block, err := ec.client.BlockByHash(context.Background(), log.BlockHash)
		if err != nil {
			// log.Printf("Failed to fetch block data: %v", err)
			return nil, err
		}

		blockTime := block.Time()
		parentHash := block.ParentHash().Hex()

		for _, topic := range log.Topics {
			if topic.Hex() == ec.topicToFilter {

				keyedLog := KeyedLog{
					RootData:   common.Bytes2Hex(log.Data),
					ParentHash: parentHash,
					BlockTime:  blockTime,
				}

				keyedLogs = append(keyedLogs, keyedLog)
			}
		}
	}

	return keyedLogs, nil
}
