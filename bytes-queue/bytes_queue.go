/*
   Copyright 20210205 allegro

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package bytesqueue

import (
	"encoding/binary"
	"errors"
	"log"
	"time"
)

const (
	// Number of bytes to encode 0 in uvarint format
	minimumHeaderSize = 17 // 1 byte blobsize + timestampSizeInBytes + hashSizeInBytes
	// Bytes before left margin are not used. Zero index means element does not exist in queue, useful while reading slice from index
	leftMarginIndex = 1
)

var (
	ErrEmptyQueue       = errors.New("Empty queue")
	ErrInvalidIndex     = errors.New("Index must be greater than zero, invalid index.")
	ErrIndexOutOfBounds = errors.New("Index out of range")
	ErrFullQueue        = errors.New("Full queue. Maximum size limit reached.")
)

// BytesQueue is a non-thread-safe queue type of fifo based on bytes array.
// For every push operation, index of entry will be returned, which can be used to read the entry later.
type BytesQueue struct {
	full              bool
	array             []byte
	capacity          uint64
	maxCapacity       uint64
	head              uint64
	tail              uint64
	rightMarginIndex  uint64
	count             int
	headerEntryBuffer []byte
	verbose           bool
}

// getUvarintSize returns the number of bytes to encode x in uvarint format.
func getUvarintSize(x uint32) uint64 {
	if x < 128 {
		return 1
	} else if x < 16384 { // 128 * 128
		return 2
	} else if x < 2097152 { // 128 * 128 * 128
		return 3
	} else if x < 268435456 { // 128 * 128 * 128 * 128
		return 4
	} else {
		return 5
	}
}

// NewBytesQueue initializes a new bytes-queue.
// Capacity is used in bytes array allocation.
// When verbose flag is set then information about memory allocation will be printed.
// TODO: 将capacity和maxCapacity设为4KB的倍数值.
func NewBytesQueue(capacity int, maxCapacity int, verbose bool) *BytesQueue {
	return &BytesQueue{
		full:              false,
		array:             make([]byte, capacity),
		capacity:          uint64(capacity),
		maxCapacity:       uint64(maxCapacity),
		head:              leftMarginIndex,
		tail:              leftMarginIndex,
		rightMarginIndex:  leftMarginIndex,
		count:             0,
		headerEntryBuffer: make([]byte, binary.MaxVarintLen32),
		verbose:           verbose,
	}
}

// Reset removes all entries from bytes-queue.
func (q *BytesQueue) Reset() {
	// Just reset indexes
	q.tail = leftMarginIndex
	q.head = leftMarginIndex
	q.rightMarginIndex = leftMarginIndex
	q.count = 0
	q.full = false
}

// Push copies entry at the end of bytes-queue and moves tail pointer.
// Allocates more space if needed.
// Returns index of pushed data or error if maximum queue size limit is reached.
func (q *BytesQueue) Push(data []byte) (int, error) {
	dataLen := uint64(len(data))
	headerEntrySize := getUvarintSize(uint32(dataLen))

	if !q.canInsertAfterTail(dataLen + headerEntrySize) {
		if q.canInsertBeforeHead(dataLen + headerEntrySize) {
			q.tail = leftMarginIndex
		} else if q.capacity+dataLen+headerEntrySize >= q.maxCapacity && q.maxCapacity > 0 {
			return -1, ErrFullQueue
		} else {
			q.allocateAdditionalMemory(dataLen + headerEntrySize)
		}
	}

	index := q.tail

	q.push(data, dataLen)

	return int(index), nil
}

func (q *BytesQueue) allocateAdditionalMemory(minimum uint64) {
	start := time.Now()

	if q.capacity < minimum {
		q.capacity += minimum
	}
	q.capacity = q.capacity * 2
	if q.capacity > q.maxCapacity && q.maxCapacity > 0 {
		q.capacity = q.maxCapacity
	}

	oldArray := q.array
	q.array = make([]byte, q.capacity)

	if leftMarginIndex != q.rightMarginIndex {
		copy(q.array, oldArray[:q.rightMarginIndex])

		if q.tail <= q.head {
			if q.tail != q.head {
				headerEntrySize := getUvarintSize(uint32(q.head - q.tail))
				emptyBlobLen := q.head - q.tail - headerEntrySize
				q.push(make([]byte, emptyBlobLen), emptyBlobLen)
			}

			q.head = leftMarginIndex
			q.tail = q.rightMarginIndex
		}
	}

	q.full = false

	if q.verbose {
		log.Printf("Allocated new bytes-queue in %s; Capacity: %d \n", time.Since(start), q.capacity)
	}
}

/*
	...| HeaderEntrySize of e(i) | e(i) | HeaderEntrySize of e(i+1) | e(i+1) | ...

	HeaderEntrySize is the uvarint encoding of len of entry.
*/
func (q *BytesQueue) push(data []byte, len uint64) {
	headerEntrySize := uint64(binary.PutUvarint(q.headerEntryBuffer, len))
	// put the len of data first
	q.copy(q.headerEntryBuffer, headerEntrySize)
	// then, put the data
	q.copy(data, len)

	if q.tail > q.head {
		// next index we can put entry
		q.rightMarginIndex = q.tail
	}
	if q.tail == q.head {
		q.full = true
	}

	q.count++
}

func (q *BytesQueue) copy(data []byte, len uint64) {
	q.tail += uint64(copy(q.array[q.tail:], data[:len]))
}

// Pop reads the oldest entry from bytes-queue and moves head pointer to the next one.
func (q *BytesQueue) Pop() ([]byte, error) {
	data, headerEntrySize, err := q.peek(q.head)
	if err != nil {
		return nil, err
	}
	size := uint64(len(data))

	q.head += headerEntrySize + size
	q.count--

	// deal with empty bytes-queue
	if q.head == q.rightMarginIndex {
		q.head = leftMarginIndex
		if q.tail == q.rightMarginIndex {
			q.tail = leftMarginIndex
		}
		q.rightMarginIndex = q.tail
	}

	q.full = false

	return data, nil
}

// Peek reads the oldest entry from bytes-queue without moving head pointer.
func (q *BytesQueue) Peek() ([]byte, error) {
	data, _, err := q.peek(q.head)
	return data, err
}

// Get reads entry at index from bytes-queue.
func (q *BytesQueue) Get(index int) ([]byte, error) {
	data, _, err := q.peek(uint64(index))
	return data, err
}

// CheckGet checks if an entry can be read at index.
func (q *BytesQueue) CheckGet(index int) error {
	return q.peekCheckErr(uint64(index))
}

// Capacity returns number of allocated bytes for bytes-queue.
func (q *BytesQueue) Capacity() int {
	return int(q.capacity)
}

// Len returns number of entries kept in bytes-queue.
func (q *BytesQueue) Len() int {
	return q.count
}

// peekCheckErr is identical to peek, but does not actually return any data.
func (q *BytesQueue) peekCheckErr(index uint64) error {
	if q.count == 0 {
		return ErrEmptyQueue
	}
	if index <= 0 {
		return ErrInvalidIndex
	}
	if index >= uint64(len(q.array)) {
		return ErrIndexOutOfBounds
	}
	// TODO: maybe we can use bloom filter to record every valid index, and check the index in more detail.
	return nil
}

// peek returns the data at index and the number of bytes to encode the length of the data in uvarint format.
func (q *BytesQueue) peek(index uint64) ([]byte, uint64, error) {
	if err := q.peekCheckErr(index); err != nil {
		return nil, 0, err
	}

	// blockSize is the length of the entry
	// n is the number of bytes to encode the length of the entry in uvarint format
	blockSize, n := binary.Uvarint(q.array[index:])
	un := uint64(n)
	return q.array[index+un : index+un+blockSize], un, nil
}

// canInsertAfterTail returns true if it's possible to insert an entry of size of need at the tail of the queue.
func (q *BytesQueue) canInsertAfterTail(need uint64) bool {
	if q.full {
		return false
	}
	if q.tail >= q.head {
		return q.capacity-q.tail >= need
	}
	// 1. there is exactly need bytes between head and tail, so we do not need
	// to reserve extra space for a potential empty entry when realloc this queue
	// 2. still have unused space between tail and head, then we must reserve
	// at least headerEntrySize bytes so we can put an empty entry
	return q.head-q.tail == need || q.head-q.tail >= need+minimumHeaderSize
}

// canInsertBeforeHead returns true if it's possible to insert an entry of size of need before the head of the queue.
func (q *BytesQueue) canInsertBeforeHead(need uint64) bool {
	if q.full {
		return false
	}
	if q.tail >= q.head {
		return q.head-leftMarginIndex == need || q.head-leftMarginIndex >= need+minimumHeaderSize
	}
	return q.head-q.tail == need || q.head-q.tail >= need+minimumHeaderSize
}
