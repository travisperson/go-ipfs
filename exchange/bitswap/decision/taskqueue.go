package decision

import (
	"fmt"
	"sync"
	"time"

	wantlist "github.com/jbenet/go-ipfs/exchange/bitswap/wantlist"
	peer "github.com/jbenet/go-ipfs/p2p/peer"
	u "github.com/jbenet/go-ipfs/util"
)

// TODO: at some point, the strategy needs to plug in here
// to help decide how to sort tasks (on add) and how to select
// tasks (on getnext). For now, we are assuming a dumb/nice strategy.
type taskQueue struct {
	// TODO: make this into a priority queue
	lock    sync.Mutex
	tasks   []*task
	taskmap map[string]*task
}

func newTaskQueue() *taskQueue {
	return &taskQueue{
		taskmap: make(map[string]*task),
	}
}

type task struct {
	Entry  wantlist.Entry
	Target peer.ID
	Trash  bool // TODO make private

	created time.Time
}

func (t *task) String() string {
	return fmt.Sprintf("<Task %s, %s, %v>", t.Target, t.Entry.Key, t.Trash)
}

// Push currently adds a new task to the end of the list
func (tl *taskQueue) Push(entry wantlist.Entry, to peer.ID) {
	tl.lock.Lock()
	defer tl.lock.Unlock()
	if task, ok := tl.taskmap[taskKey(to, entry.Key)]; ok {
		// TODO: when priority queue is implemented,
		//       rearrange this task
		task.Entry.Priority = entry.Priority
		return
	}
	task := &task{
		Entry:   entry,
		Target:  to,
		created: time.Now(),
	}
	tl.tasks = append(tl.tasks, task)
	tl.taskmap[taskKey(to, entry.Key)] = task
}

// Pop 'pops' the next task to be performed. Returns nil no task exists.
func (tl *taskQueue) Pop() *task {
	tl.lock.Lock()
	defer tl.lock.Unlock()
	var out *task
	for len(tl.tasks) > 0 {
		// TODO: instead of zero, use exponential distribution
		//       it will help reduce the chance of receiving
		//		 the same block from multiple peers
		out = tl.tasks[0]
		tl.tasks = tl.tasks[1:]
		delete(tl.taskmap, taskKey(out.Target, out.Entry.Key))
		if out.Trash {
			continue // discarding tasks that have been removed
		}
		break // and return |out|
	}
	return out
}

// Remove lazily removes a task from the queue
func (tl *taskQueue) Remove(k u.Key, p peer.ID) {
	tl.lock.Lock()
	t, ok := tl.taskmap[taskKey(p, k)]
	if ok {
		t.Trash = true
	}
	tl.lock.Unlock()
}

// taskKey returns a key that uniquely identifies a task.
func taskKey(p peer.ID, k u.Key) string {
	return string(p) + string(k)
}
