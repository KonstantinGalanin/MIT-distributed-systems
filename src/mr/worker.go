package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

type KeyValue struct {
	Key   string
	Value string
}

type Actions struct {
	Mapf    func(string, string) []KeyValue
	Reducef func(string, []string) string
}

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	actions := &Actions{mapf, reducef}
	for {
		time.Sleep(5 * time.Millisecond)
		RequestTask(actions)
	}
}

func RequestTask(actions *Actions) {
	reply := &Task{}
	ok := call("Coordinator.RequestTask", &TaskResponse{}, reply)

	if !ok {
		os.Exit(0)
	}

	switch reply.TaskType {
	case MapTask:
		MapExec(reply, actions.Mapf)
	case ReduceTask:
		ReduceExec(reply, actions.Reducef)
	default:
		time.Sleep(5 * time.Second)
		return
	}

	args := &TaskResponse{
		TaskID: reply.TaskID,
		TaskCondition: TaskDone,
		TaskType: reply.TaskType,
	}
	call("Coordinator.ResponseTask", args, &Task{})
}

func MapExec(task *Task, mapf func(string, string) []KeyValue) error {
	intermediate := make([][]KeyValue, task.NReduce)

	file, err := os.Open(task.FileName)
	if err != nil {
		return fmt.Errorf("Open file error: %w", err)
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		return fmt.Errorf("Read file error: %w", err)
	}

	kva := mapf(task.FileName, string(content))
	for _, kv := range kva {
		bucket := ihash(kv.Key) % task.NReduce
		intermediate[bucket] = append(intermediate[bucket], kv)
	}

	for bucket := range intermediate {
		tmpFileName := fmt.Sprintf("mr-%v-%v", bucket, task.TaskID)
		// file, err := os.Create(tmpFileName)
		file, err := os.CreateTemp("", tmpFileName)
		if err != nil {
			return fmt.Errorf("Open tmp file error: %w", err)
		}
		defer file.Close()

		enc := json.NewEncoder(file)
		for _, kv := range intermediate[bucket] {
			err := enc.Encode(kv)
			if err != nil {
				return fmt.Errorf("Encoding kv to json error: %w", err)
			}
		}
		file.Close()
		os.Rename(file.Name(), tmpFileName)
	}

	return nil
}


func getIntermediateData(task *Task) ([]KeyValue, error) {
	intermediate := []KeyValue{}
	for i := 0; i < task.NMap; i++ {
		fileName := fmt.Sprintf("mr-%v-%v", task.TaskID, i)
		file, err := os.Open(fileName)
		if err != nil {
			return nil, fmt.Errorf("Open tmp file error: %w", err)
		}
		defer os.Remove(fileName)
		defer file.Close()

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key <= intermediate[j].Key
	})

	return intermediate, nil
}

func ReduceExec(task *Task, reducef func(string, []string) string) error {
	intermediate, err := getIntermediateData(task)
	if err != nil {
		return err
	}

	fileName := fmt.Sprintf("mr-out-%v", task.TaskID)
	file, err := os.Create(fileName)
	if err != nil {
		return fmt.Errorf("Create file error: %w", err)
	}
	defer file.Close()

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(file, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	return nil
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		os.Exit(0)
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	return false
}
