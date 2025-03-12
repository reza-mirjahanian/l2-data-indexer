package db

import (
	"errors"
	"fmt"
	"os"

	"go.etcd.io/bbolt"
)

// Common errors
var (
	ErrBucketNotFound = errors.New("bucket not found")
	ErrKeyNotFound    = errors.New("key not found")
	ErrEmptyKey       = errors.New("key cannot be empty")
	ErrEmptyValue     = errors.New("value cannot be empty")
)

// DBConfig holds configuration for the database
type DBConfig struct {
	Path     string
	FileMode os.FileMode
	Options  *bbolt.Options
}

// BoltDBWrapper provides a simplified interface to interact with BoltDB
type BoltDBWrapper struct {
	db *bbolt.DB
}

// NewBoltDB creates and initializes a new BoltDB instance
func NewBoltDB(config DBConfig) (*BoltDBWrapper, error) {
	if config.Path == "" {
		return nil, errors.New("database path cannot be empty")
	}

	if config.FileMode == 0 {
		config.FileMode = 0600 // Default file permissions
	}

	db, err := bbolt.Open(config.Path, config.FileMode, config.Options)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	return &BoltDBWrapper{db: db}, nil
}

// Close safely closes the database connection
func (b *BoltDBWrapper) Close() error {
	if b.db == nil {
		return nil
	}

	if err := b.db.Close(); err != nil {
		return fmt.Errorf("failed to close database: %w", err)
	}
	return nil
}

// WriteToDB writes a key-value pair to the specified bucket
func (b *BoltDBWrapper) WriteToDB(bucketName, key, value string) error {
	if err := b.validateInputs(bucketName, key, value); err != nil {
		return err
	}

	return b.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := b.getOrCreateBucket(tx, bucketName)
		if err != nil {
			return err
		}

		return bucket.Put([]byte(key), []byte(value))
	})
}

// ReadFromDB retrieves a value from the specified bucket and key
func (b *BoltDBWrapper) ReadFromDB(bucketName, key string) (string, error) {
	if err := b.validateInputs(bucketName, key, ""); err != nil {
		return "", err
	}

	var value string
	err := b.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		if bucket == nil {
			return ErrBucketNotFound
		}

		v := bucket.Get([]byte(key))
		if v == nil {
			return ErrKeyNotFound
		}

		value = string(v)
		return nil
	})

	if err != nil {
		return "", err
	}

	return value, nil
}

// DeleteFromDB removes a key-value pair from the specified bucket
func (b *BoltDBWrapper) DeleteFromDB(bucketName, key string) error {
	if err := b.validateInputs(bucketName, key, ""); err != nil {
		return err
	}

	return b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		if bucket == nil {
			return ErrBucketNotFound
		}

		return bucket.Delete([]byte(key))
	})
}

// BatchWrite performs multiple writes in a single transaction
func (b *BoltDBWrapper) BatchWrite(bucketName string, entries map[string]string) error {
	if bucketName == "" {
		return errors.New("bucket name cannot be empty")
	}

	if len(entries) == 0 {
		return errors.New("entries map cannot be empty")
	}

	return b.db.Batch(func(tx *bbolt.Tx) error {
		bucket, err := b.getOrCreateBucket(tx, bucketName)
		if err != nil {
			return err
		}

		for key, value := range entries {
			if err := b.validateInputs("", key, value); err != nil {
				return err
			}

			if err := bucket.Put([]byte(key), []byte(value)); err != nil {
				return fmt.Errorf("failed to write key %s: %w", key, err)
			}
		}
		return nil
	})
}

// Helper functions

func (b *BoltDBWrapper) validateInputs(bucketName, key, value string) error {
	if bucketName != "" && len(bucketName) == 0 {
		return errors.New("bucket name cannot be empty")
	}

	if len(key) == 0 {
		return ErrEmptyKey
	}

	if value != "" && len(value) == 0 {
		return ErrEmptyValue
	}

	return nil
}

func (b *BoltDBWrapper) getOrCreateBucket(tx *bbolt.Tx, bucketName string) (*bbolt.Bucket, error) {
	bucket := tx.Bucket([]byte(bucketName))
	if bucket == nil {
		var err error
		bucket, err = tx.CreateBucket([]byte(bucketName))
		if err != nil {
			return nil, fmt.Errorf("failed to create bucket %s: %w", bucketName, err)
		}
	}
	return bucket, nil
}

// ForEachInBucket iterates over all key-value pairs in a bucket
func (b *BoltDBWrapper) ForEachInBucket(bucketName string, fn func(k, v []byte) error) error {
	if bucketName == "" {
		return errors.New("bucket name cannot be empty")
	}

	return b.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		if bucket == nil {
			return ErrBucketNotFound
		}

		return bucket.ForEach(fn)
	})
}
