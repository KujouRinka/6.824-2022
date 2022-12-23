package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	nReduce int
	// task sending definition
	taskQueue chan Task
	taskWg    sync.WaitGroup
	// task id counter
	taskIdCounter int
	// task notification record
	taskNotifyMap  map[int]chan struct{}
	taskNotifyLock sync.Mutex
	// done flag
	done     bool
	doneLock sync.Mutex
}

func (c *Coordinator) NewTaskId() int {
	c.taskIdCounter++
	return c.taskIdCounter
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) NeedWork(args *NeedWorkArgs, reply *NeedWorkReply) error {
	task, ok := <-c.taskQueue
	if !ok {
		return fmt.Errorf("cannot get task from task queue")
	}
	reply.T = task
	reply.ReduceCnt = c.nReduce
	// make task notification channel
	c.taskNotifyLock.Lock()
	c.taskNotifyMap[task.TaskId] = make(chan struct{})
	// set timer for task
	go func(taskChan chan struct{}) {
		select {
		case <-taskChan:
			// task done
			return
		case <-time.After(10 * time.Second):
			c.taskQueue <- task
		}
	}(c.taskNotifyMap[task.TaskId])
	c.taskNotifyLock.Unlock()

	return nil
}

func (c *Coordinator) FinishWork(args *FinishWorkArgs, reply *FinishWorkReply) error {
	taskId := args.TaskId
	c.taskNotifyLock.Lock()
	notifyChan, ok := c.taskNotifyMap[taskId]
	if !ok {
		c.taskNotifyLock.Unlock()
		return nil
	}
	// notify task done
	notifyChan <- struct{}{}
	c.taskWg.Done()
	// log.Printf("task %d done\n", taskId)
	// delete task notification channel
	delete(c.taskNotifyMap, taskId)
	c.taskNotifyLock.Unlock()
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.doneLock.Lock()
	ret := c.done
	c.doneLock.Unlock()

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce:       nReduce,
		taskQueue:     make(chan Task, 100),
		taskWg:        sync.WaitGroup{},
		taskIdCounter: 0,
		taskNotifyMap: make(map[int]chan struct{}),
		done:          false,
	}
	// log.Printf("%d file found\n", len(files))
	c.taskWg.Add(len(files))

	// Your code here.

	// make send task goroutine
	go func(files []string) {
		// send map task
		for _, filename := range files {
			c.taskQueue <- NewMapTask(filename, c.NewTaskId())
		}
		// log.Println("all task sent, waiting for next reduce task")
		// wait for all map task done
		c.taskWg.Wait()
		// log.Println("start sending reduce task")
		// send reduce task
		c.taskWg.Add(nReduce)
		// make Done() check goroutine
		// log.Println("waiting exit goroutine created")
		go func() {
			c.taskWg.Wait()
			c.doneLock.Lock()
			c.done = true
			c.doneLock.Unlock()
		}()
		// log.Println("sending reduce task")
		for i := 0; i < nReduce; i++ {
			c.taskQueue <- NewReduceTask(i, c.NewTaskId())
		}
	}(files)

	c.server()
	return &c
}
