package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	TaskUnassigned = iota
	TaskInProgress
	TaskDone
)

type TaskItem struct {
	TaskCondition int
	TaskId        int
	FileName      string
	StartTime     time.Time
}

type TaskID = int

type Coordinator struct {
	files           []string
	NMap            int
	NReduce         int
	MapTasks        map[TaskID]*TaskItem
	ReduceTasks     map[TaskID]*TaskItem
	MapTasksDone    bool
	ReduceTasksDone bool
	mu              sync.Mutex
}

func (c *Coordinator) checkTasksDone(tasks map[TaskID]*TaskItem) bool {
	for _, task := range tasks {
		if task.TaskCondition != TaskDone {
			return false
		}
	}

	return true
}

func (c *Coordinator) RequestTask(args *TaskResponse, reply *Task) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.MapTasksDone {
		countDone := 0
		for _, mapTask := range c.MapTasks {
			if mapTask.TaskCondition == TaskUnassigned || mapTask.TaskCondition == TaskInProgress && time.Since(mapTask.StartTime) > 10 * time.Second {
				{
					reply.TaskID = mapTask.TaskId
					reply.TaskType = MapTask
					reply.FileName = c.files[mapTask.TaskId]
					reply.NMap = c.NMap
					reply.NReduce = c.NReduce
				}
				mapTask.TaskCondition = TaskInProgress
				mapTask.StartTime = time.Now()
				break
			} else if mapTask.TaskCondition == TaskDone {
				countDone++
			}
		}
		if len(c.MapTasks) == countDone {
			c.MapTasksDone = true
		}
	}

	if c.MapTasksDone {
		for _, reduceTask := range c.ReduceTasks {
			if reduceTask.TaskCondition == TaskUnassigned || reduceTask.TaskCondition == TaskInProgress && time.Since(reduceTask.StartTime) > 10 * time.Second {
				{
					reply.TaskID = reduceTask.TaskId
					reply.TaskType = ReduceTask
					reply.NMap = c.NMap
					reply.NReduce = c.NReduce
				}
				reduceTask.TaskCondition = TaskInProgress
				reduceTask.StartTime = time.Now()
				break
			}
		}
	}

	return nil
}

func (c *Coordinator) ResponseTask(args *TaskResponse, reply *Task) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch args.TaskType {
	case MapTask:
		c.MapTasks[args.TaskID].TaskCondition = args.TaskCondition
	case ReduceTask:
		c.ReduceTasks[args.TaskID].TaskCondition = args.TaskCondition
	}

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
	reduceTasksDone := c.checkTasksDone(c.ReduceTasks)

	return reduceTasksDone
}

// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:           files,
		NMap:            len(files),
		NReduce:         nReduce,
		MapTasks:        make(map[int]*TaskItem),
		ReduceTasks:     make(map[int]*TaskItem),
		MapTasksDone:    false,
		ReduceTasksDone: false,
		mu:              sync.Mutex{},
	}

	for id := 0; id < c.NMap; id++ {
		task := &TaskItem{
			TaskCondition: TaskUnassigned,
			TaskId:        id,
		}
		c.MapTasks[id] = task
	}

	for id := 0; id < c.NReduce; id++ {
		task := &TaskItem{
			TaskCondition: TaskUnassigned,
			TaskId:        id,
		}
		c.ReduceTasks[id] = task
	}

	c.server()
	return &c
}
