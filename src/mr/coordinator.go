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

type CoordinatorTaskStatus int

const (
	Idle CoordinatorTaskStatus = iota
	InProgress
	Completed
)

type State int

const (
	Map State = iota
	Reduce
	Exit
	Wait
)

type CoorinatorTask struct {
	TaskStatus    CoordinatorTaskStatus
	StartTime     time.Time
	TaskReference *Task
}

// nreduce number of reduce shard
type Coordinator struct {
	// Your definitions here.
	TaskQueue        chan *Task
	TaskMeta         map[int]*CoorinatorTask // task number -> task
	CoordinatorPhase State
	Nreduce          int
	InputFiles       []string
	Intermediates    [][]string
}

// map or reduce的worker状态
type Task struct {
	Input         string
	TaskState     State
	NReducer      int
	TaskNumber    int // task的编号
	Intermediates []string
	Output        string
}

var mu sync.Mutex

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	mu.Lock()
	defer mu.Unlock()
	ret := c.CoordinatorPhase == Exit
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{
		TaskQueue:        make(chan *Task, max(len(files), nReduce)),
		TaskMeta:         make(map[int]*CoorinatorTask),
		CoordinatorPhase: Map,
		Nreduce:          nReduce,
		InputFiles:       files,
		Intermediates:    make([][]string, nReduce),
	}
	// 创建map任务
	c.createMapTask()
	// 一个程序成为master，其他成为worker
	//这里就是启动master 服务器就行了，
	//拥有master代码的就是master，别的发RPC过来的都是worker
	c.server()

	// 启动一个goroutine 检查超时任务
	go c.catchTimeout()
	return &c
}

func (c *Coordinator) createMapTask() {
	for idx, filename := range c.InputFiles {
		taskMeta := Task{
			Input:      filename,
			TaskState:  Map,
			NReducer:   c.Nreduce,
			TaskNumber: idx,
		}
		c.TaskQueue <- &taskMeta
		c.TaskMeta[idx] = &CoorinatorTask{
			TaskStatus:    Idle,
			TaskReference: &taskMeta,
		}
	}
}

func (c *Coordinator) AssignTask(args *ExampleArgs, replay *Task) error {
	// assignTask就看看自己queue里面还有没有task
	mu.Lock()
	defer mu.Unlock()
	if len(c.TaskQueue) > 0 {
		// 有就发送任务
		*replay = *<-c.TaskQueue
		c.TaskMeta[replay.TaskNumber].TaskStatus = InProgress
		c.TaskMeta[replay.TaskNumber].StartTime = time.Now()
	} else if c.CoordinatorPhase == Exit {
		replay = &Task{TaskState: Exit}
	} else {
		// 没有任务就waitting
		replay = &Task{TaskState: Wait}
	}
	return nil
}

func (c *Coordinator) TaskCompleted(task *Task, reply *ExampleReply) error {
	// update task status
	mu.Lock()
	defer mu.Unlock()
	if task.TaskState != c.CoordinatorPhase || c.TaskMeta[task.TaskNumber].TaskStatus == Completed {
		return nil
	}
	c.TaskMeta[task.TaskNumber].TaskStatus = Completed
	go c.processTaskResult(task)
	return nil
}

func (c *Coordinator) processTaskResult(task *Task) {
	mu.Lock()
	defer mu.Unlock()
	switch task.TaskState {
	case Map:
		//收集intermediate信息
		for reduceTaskID, filePath := range task.Intermediates {
			c.Intermediates[reduceTaskID] = append(c.Intermediates[reduceTaskID], filePath)
		}
		if c.allTaskDone() {
			// 获取所有map task后， 进入reduce阶段
			c.createReduceTask()
			c.CoordinatorPhase = Reduce
		}
	case Reduce:
		if c.allTaskDone() {
			c.CoordinatorPhase = Exit
		}
	}
}

func (c *Coordinator) allTaskDone() bool {
	for _, v := range c.TaskMeta {
		if v.TaskStatus != Completed {
			return false
		}
	}
	return true
}

func (c *Coordinator) createReduceTask() {
	c.TaskMeta = make(map[int]*CoorinatorTask)
	for idx, files := range c.Intermediates {
		taskMeta := &Task{
			TaskState:     Reduce,
			NReducer:      c.Nreduce,
			TaskNumber:    idx,
			Intermediates: files,
		}
		c.TaskQueue <- taskMeta
		c.TaskMeta[idx] = &CoorinatorTask{
			TaskStatus:    Idle,
			TaskReference: taskMeta,
		}
	}
}

func (c *Coordinator) catchTimeout() {
	for {
		time.Sleep(5 * time.Second)
		mu.Lock()
		if c.CoordinatorPhase == Exit {
			mu.Unlock()
			return
		}
		for _, cTask := range c.TaskMeta {
			if cTask.TaskStatus == InProgress && time.Now().Sub(cTask.StartTime) > 10*time.Second {
				c.TaskQueue <- cTask.TaskReference
				cTask.TaskStatus = Idle
			}
		}
		mu.Unlock()
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
