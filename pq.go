package priorty_queue

import (
	"container/heap"
	"fmt"
)

// This priority queue manages eventBuffers that expire after a certain
// period of inactivity (no new events).
type Item struct {
	value string
	// The priority of the item in the queue.
	// For our purposes, this is milliseconds since epoch
	priority int64
	// index is needed by update and is maintained by heap.Interface
	// The index of this item in the heap.
	index int
}

// A Priority Queue (min heap) implemented with go's heap container.
// Adapted from go's example at: https://golang.org/pkg/container/heap/
//
// This priorityQueue is used to keep track of eventBuffer objects in order of
// oldest last-event-timestamp so that we can more efficiently purge buffers
// that have expired events.
//
// The priority here will be a timestamp in milliseconds since epoch (int64)
// with lower values (older timestamps) being at the top of the heap/queue and
// higher values (more recent timestamps) being further down.
// So this is a Min Heap.
//
// A priorityQueue implements heap.Interface and holds Items.
type PriorityQueue []*Item

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return pq[i].priority > pq[j].priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Item)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// update modifies the priority of an item and updates the heap accordingly
func (pq *PriorityQueue) Update(item *Item, priority int64) {
	item.priority = priority
	// NOTE: fix is a slightly more efficient version of calling Remove() and
	// then Push()
	heap.Fix(pq, item.index)
}

// get the priority of the heap's top item.
func (pq *PriorityQueue) peakTopPriority() (int64, error) {
	if len(*pq) > 0 {
		return (*pq)[0].priority, nil
	} else {
		return -1, fmt.Errorf("PriorityQueue is empty.  No top priority.")
	}
}
