package connectpool

import (
	"mi/events"
	"mi/tcpserve"
	"net"
)

type Task struct {
	tcpconn  net.Conn
	msgcount int
	task     func(conn net.Conn) error
}

func NewTask(arg_task func(conn net.Conn) error, conn net.Conn) *Task {
	t := Task{
		tcpconn:  conn,
		msgcount: 0,
		task:     arg_task,
	}
	return &t
}

func (t *Task) Execute() {
	t.task(t.tcpconn)
}

type Pool struct {
	EntryChannel chan *Task
	JobsChannel  chan *Task
	eventBus     *events.EventBus
	work_num     int
}

func NewPool(worker_max_num int) *Pool {
	t := Pool{
		EntryChannel: make(chan *Task),
		JobsChannel:  make(chan *Task),
		work_num:     worker_max_num,
		eventBus:     events.NewEventBus(),
	}
	return &t
}

func (p *Pool) worker(worker_id int) {
	for task := range p.JobsChannel {
		task.Execute()
	}
}

func (p *Pool) Clone() {

}

func (p *Pool) CloneAll() {

}

func (p *Pool) Run() {
	for i := 0; i < p.work_num; i++ {
		go p.worker(i)
	}

	for task := range p.EntryChannel {
		p.JobsChannel <- task
	}
}

func Handle(handle *tcpserve.Handle, conn net.Conn, pool *Pool) {
	task := NewTask(handle.Process, conn)
	pool.EntryChannel <- task
}
