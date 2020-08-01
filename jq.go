package qup

import (
	"encoding/gob"
	"errors"
	"github.com/prasanthmj/sett"
	"reflect"
	"sync"
	"time"
)

type TaskExecutor interface {
	Execute(interface{}) error
}

type JobQueue struct {
	// sync variables - used for syncing - concurrency aware objects
	jobs  chan string
	close chan struct{}
	wg    sync.WaitGroup

	//startup variables: that are used only while starting up
	NumWorkers int
	dataFolder string
	tickPeriod time.Duration

	//variables used throughout
	access    *sync.RWMutex
	store     *sett.Sett
	ticker    *time.Ticker
	started   bool
	executors map[string]TaskExecutor

	log Logger
}

func NewJobQueue() *JobQueue {

	d := &JobQueue{NumWorkers: 10, log: &emptyLogger{}}
	d.executors = make(map[string]TaskExecutor)
	d.access = &sync.RWMutex{}
	return d
}
func (d *JobQueue) DataFolder(df string) *JobQueue {
	d.dataFolder = df
	return d
}

func (d *JobQueue) Workers(n int) *JobQueue {
	d.NumWorkers = n
	return d
}
func (d *JobQueue) Logger(l Logger) *JobQueue {
	if l == nil {
		d.log = &emptyLogger{}
	} else {
		d.log = l
	}

	return d
}

func (d *JobQueue) Register(t interface{}, e TaskExecutor) {
	gob.Register(t)
	name := reflect.TypeOf(t).String()
	d.executors[name] = e
}

func (d *JobQueue) TickPeriod(tp time.Duration) *JobQueue {
	d.tickPeriod = tp
	return d
}

func (d *JobQueue) Start() error {
	if d.isStarted() {
		return errors.New("The queue was already started.")
	}

	if len(d.dataFolder) <= 0 {
		return errors.New("Datafolder shouldn't be empty!")
	}

	storeOpts := sett.DefaultOptions(d.dataFolder)
	storeOpts.Logger = nil
	d.store = sett.Open(storeOpts)
	if d.NumWorkers <= 0 {
		d.NumWorkers = 10
	}
	d.jobs = make(chan string, d.NumWorkers)
	d.close = make(chan struct{})
	if d.tickPeriod <= 0 {
		d.tickPeriod = 1 * time.Second
	}
	d.ticker = time.NewTicker(d.tickPeriod)
	for i := 0; i < d.NumWorkers; i++ {
		go d.worker(i + 1)
	}
	d.markStarted(true)
	return nil
}

func (d *JobQueue) worker(wid int) {
	d.log.Logf("Worker %d starting\n", wid)
	d.wg.Add(1)
	defer d.wg.Done()
	for {
		select {
		case jid := <-d.jobs:
			d.runTask(wid, jid)
		case <-d.ticker.C:
			d.periodicChecks()
		case <-d.close:
			d.log.Logf("Workder %d stopping", wid)
			return
		}
	}
}

func (d *JobQueue) runTask(wid int, jid string) {
	j, err := d.store.Table("jobqueue.ready").Cut(jid)
	if err != nil {
		d.log.Errorf("Couldn't cut job item from sett id %s error %v ", jid, err)
		return
	}
	job, ok := j.(*Job)
	if !ok {
		d.log.Errorf("Received object from queue that does not convert to Job")
		return
	}

	//Schedules a job if it is recurring
	defer d.scheduleRecurringJob(job)

	name := reflect.TypeOf(job.Task).String()
	d.access.RLock()
	exec, exists := d.executors[name]
	d.access.RUnlock()
	if !exists {
		d.log.Errorf("Executor for type %s not registered", name)
		return
	}
	err = exec.Execute(job.Task)
	if err != nil {
		d.log.Errorf("Error while executing task executor %s error %v", name, err)
		return
	}

}

func (d *JobQueue) periodicChecks() {
	d.log.Logf("periodicChecks running ...")

	scheduledTable := d.store.Table("jobqueue.scheduled")
	readyTable := d.store.Table("jobqueue.ready")

	jobsdue, err := scheduledTable.Filter(
		func(k string, j interface{}) bool {
			job := j.(*Job)
			if job.IsDue() {
				return true
			}
			return false
		})
	if err != nil {
		d.log.Errorf("Error getting scheduled jobs %v ", err)
		return
	}
	d.log.Logf(" %d scheduled jobs Due for running ", len(jobsdue))
	for _, k := range jobsdue {
		j, err := scheduledTable.Cut(k)
		if err != nil {
			d.log.Errorf("Error in Cut() job key %s job %v ", k, err)
			continue
		}

		job := j.(*Job)

		// Why not execute the job immediately?
		// When there are many scheduled jobs ready to go, that will
		// make the execution sequential. This periodic check should
		// complete as soon as possible. So just push to ready queue and be done with that
		jid, err := readyTable.Insert(job)
		if err != nil {
			d.log.Errorf("Error inserting job to queue %v ", err)
			continue
		}
		isClosed := d.signalNewJob(jid)
		if isClosed {
			//Close signal; will do the scheduled job processing later
			//return home immediately
			return
		}
	}
}
func (d *JobQueue) signalNewJob(jid string) bool {
	//check whether already closed
	select {
	case <-d.close:
		return false
	default:
	}

	select {
	case <-d.close:
		return false
	default:
		d.jobs <- jid
	}
	return true
}

func (d *JobQueue) isStarted() bool {
	d.access.Lock()
	defer d.access.Unlock()
	return d.started
}
func (d *JobQueue) markStarted(started bool) {
	d.access.Lock()
	defer d.access.Unlock()
	d.started = started
}
func (d *JobQueue) Stop() error {
	if !d.isStarted() {
		return errors.New("The JobQueue is not started ")
	}
	if d.ticker != nil {
		d.ticker.Stop()
	}

	close(d.close)

	d.wg.Wait()
	d.store.Close()

	d.markStarted(false)
	return nil
}
func (d *JobQueue) isDuplicate(j *Job) bool {
	scheduledTable := d.store.Table("jobqueue.scheduled")
	search, err := scheduledTable.Filter(
		func(k string, j interface{}) bool {
			job := j.(*Job)
			if job.AreYouSame(job) {
				return true
			}
			return false
		})
	if err != nil {
		d.log.Errorf("Error while searching the job queue %v", err)
		return false
	}
	if len(search) > 0 {
		return true
	}
	return false
}

func (d *JobQueue) scheduleRecurringJob(job *Job) error {
	if job.IsRecurring() {
		//We won't allow Task of same type in recurring
		// If a task od one type is already scheduled
		// We won't allow another one
		if d.isDuplicate(job) {
			return nil
		}
		job.ScheduleNextDue()
		_, err := d.store.Table("jobqueue.scheduled").Insert(job)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *JobQueue) QueueUp(j *Job) error {

	if j.IsRecurring() {
		return d.scheduleRecurringJob(j)
	}

	if j.IsScheduled() {
		_, err := d.store.Table("jobqueue.scheduled").Insert(j)
		if err != nil {
			return err
		}
	} else {
		jid, err := d.store.Table("jobqueue.ready").Insert(j)
		if err != nil {
			d.log.Logf("Error returned in QueueUp %v", err)
			return err
		}
		d.log.Logf("Job ID is  %v", jid)
		d.signalNewJob(jid)
		//d.jobs <- jid
	}

	return nil
}
