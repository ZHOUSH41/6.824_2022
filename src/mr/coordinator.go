package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

const MaxRetryTime = time.Second * 10

type SchedulePhase int

const (
	MapPhase SchedulePhase = iota
	ReducePhase
	CompletePhase
)

type TaskStatus int

const (
	Idle TaskStatus = iota
	Working
	Finished
)

type JobTypeStatus int

const (
	MapJob JobTypeStatus = iota
	ReduceJob
	WaitJob
	CompleteJob
)

type Task struct {
	fileName  string
	id        int
	startTime time.Time
	status    TaskStatus
}

type heartbeatMsg struct {
	response *HeartbeatResponse
	ok       chan struct{}
}

type reportMsg struct {
	request *ReportRequest
	ok      chan struct{}
}

type Coordinator struct {
	// Your definitions here.
	files   []string
	nReduce int
	nMap    int
	phase   SchedulePhase
	tasks   []Task

	heartbeatCh chan heartbeatMsg
	reportCh    chan reportMsg
	doneCh      chan struct{}
}

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

	<-c.doneCh
	return true
}

func (c *Coordinator) schedule() {
	c.initMapPhase()
	for {
		select {
		case msg := <-c.heartbeatCh:
			if c.phase == CompletePhase {
				msg.response.JobType = CompleteJob
			} else if c.selectTask(msg.response) {
				switch c.phase {
				case MapPhase:
					log.Printf("Coord finished %v phase, turn to %v phase", MapPhase, ReducePhase)
					c.initReducePhase()
					c.selectTask(msg.response)
				case ReducePhase:
					log.Printf("Coord finished %v phase, turn to %v phase", ReducePhase, CompletePhase)
					c.initCompletedPhase()
					msg.response.JobType = CompleteJob
				case CompletePhase:
					panic(fmt.Sprintf("unexpected coord phase %v", CompletePhase))
				}
			}
			log.Printf("Coord response %v", msg.response)
			msg.ok <- struct{}{}
		case msg := <-c.reportCh:
			if msg.request.Phase == c.phase {
				log.Printf("Phase %v task %v finished", c.phase, msg.request.Id)
				c.tasks[msg.request.Id].status = Finished
			}
			msg.ok <- struct{}{}
		}
	}
}

func (c *Coordinator) selectTask(response *HeartbeatResponse) bool {
	allFinished, hasNewTask := true, false
	for index, task := range c.tasks {
		switch task.status {
		case Idle:
			allFinished, hasNewTask = false, true
			c.tasks[index].status = Working
			c.tasks[index].startTime = time.Now()
			response.Id = index
			response.NMap = c.nMap
			response.NReduce = c.nReduce
			if c.phase == MapPhase {
				response.FileName = c.tasks[index].fileName
				response.JobType = MapJob
			} else {
				response.JobType = ReduceJob
			}
		case Working:
			allFinished = false
			if time.Since(c.tasks[index].startTime) > MaxRetryTime {
				hasNewTask = true
				c.tasks[index].startTime = time.Now()
				response.Id = index
				response.NMap = c.nMap
				response.NReduce = c.nReduce
				if c.phase == MapPhase {
					response.FileName = c.tasks[index].fileName
					response.JobType = MapJob
				} else {
					response.JobType = ReduceJob
				}
			}
		case Finished:
		}
		if hasNewTask {
			break
		}
	}
	if !hasNewTask {
		response.JobType = WaitJob
	}
	return allFinished
}

func (c *Coordinator) initMapPhase() {
	c.phase = MapPhase
	c.tasks = make([]Task, len(c.files))
	for i := 0; i < c.nMap; i++ {
		c.tasks[i] = Task{
			fileName: c.files[i],
			id:       i,
			status:   Idle,
		}
	}
}

func (c *Coordinator) initReducePhase() {
	c.phase = ReducePhase
	c.tasks = make([]Task, c.nReduce)
	for i := 0; i < c.nReduce; i++ {
		c.tasks[i] = Task{
			id:     i,
			status: Idle,
		}
	}
}

func (c *Coordinator) initCompletedPhase() {
	c.phase = CompletePhase
	c.doneCh <- struct{}{}
}

func (c *Coordinator) Heartbeat(request *HeartbeatRequest, response *HeartbeatResponse) error {
	msg := heartbeatMsg{response, make(chan struct{})}
	c.heartbeatCh <- msg
	<-msg.ok
	return nil
}

func (c *Coordinator) Report(request *ReportRequest, response *ReportResponse) error {
	msg := reportMsg{request, make(chan struct{})}
	c.reportCh <- msg
	<-msg.ok
	return nil
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:       files,
		nReduce:     nReduce,
		nMap:        len(files),
		heartbeatCh: make(chan heartbeatMsg),
		reportCh:    make(chan reportMsg),
		doneCh:      make(chan struct{}),
	}

	// Your code here.

	c.server()
	go c.schedule()
	return &c
}
