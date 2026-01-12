package id

import (
	"errors"
	"sync"
	"time"
)

const (
	nodeBits        = 10
	stepBits        = 12
	nodeMax         = -1 ^ (-1 << nodeBits)
	stepMax         = -1 ^ (-1 << stepBits)
	timeShift       = nodeBits + stepBits
	nodeShift       = stepBits
	epoch     int64 = 1704067200000 // 2024-01-01 00:00:00 UTC
)

// Node holds data for the Snowflake generator
type Node struct {
	mu        sync.Mutex
	timestamp int64
	nodeID    int64
	step      int64
}

// NewNode creates a new Snowflake node
func NewNode(nodeID int64) (*Node, error) {
	if nodeID < 0 || nodeID > nodeMax {
		return nil, errors.New("node ID to large")
	}
	return &Node{
		timestamp: 0,
		nodeID:    nodeID,
		step:      0,
	}, nil
}

// Generate creates a unique ID
func (n *Node) Generate() int64 {
	n.mu.Lock()
	defer n.mu.Unlock()

	now := time.Now().UnixMilli()

	if now < n.timestamp {
		// Clock regressed, refuse to generate ID to prevent duplicates
		// In production, we might want to wait or panic
		now = n.timestamp
	}

	if now == n.timestamp {
		n.step = (n.step + 1) & stepMax
		if n.step == 0 {
			// Sequence exhausted for this millisecond, wait for next
			for now <= n.timestamp {
				now = time.Now().UnixMilli()
			}
		}
	} else {
		n.step = 0
	}

	n.timestamp = now

	return ((now - epoch) << timeShift) | (n.nodeID << nodeShift) | n.step
}
