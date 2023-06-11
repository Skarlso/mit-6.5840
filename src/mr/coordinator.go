package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	// Your definitions here.
	Tasks [][]string
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) FetchWork(args *TaskArgs, reply *Task) error {
	fmt.Println("sending work to worker: ", args.TaskID)
	// TODO: somehow use the worker's identity?
	reply.Files = c.Tasks[0]
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	fmt.Println("starting the coordinator.")
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}

	fmt.Println("registered the socket, running the server...")
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// change this once all tasks are done
	ret := false

	// store the tasks ? Where do they come from? -> They come from the number of files.
	// The number of tasks is nReduce.
	// Once 10 tasks are done, set ret to `true`

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	fmt.Println("starting with files and reduce value: ", files, nReduce)
	tasks := make([][]string, 0)

	steps := 0

	if nReduce > len(files) {
		steps = 1
	} else {
		steps = len(files) / nReduce
	}

	fmt.Println("number of reduce tasks: ", steps)

	i := 0
	for {
		if i+steps < len(files) {
			tasks = append(tasks, files[i:i+steps])
		} else if i+steps >= len(files) && i < len(files) {
			tasks = append(tasks, files[i:])
		} else {
			break
		}

		i += steps
	}

	fmt.Println("created tasks: ", tasks)

	c := Coordinator{
		Tasks: tasks,
	}

	c.server()
	return &c
}
