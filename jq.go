package qup

import (
	"encoding/gob"
	"errors"
	"fmt"
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
	jobs             chan string
	close            chan struct{}
	wg               sync.WaitGroup
	ticker           *time.Ticker
	recurringTickers []*time.Ticker

	//startup variables: that are used only while starting up
	NumWorkers  int
	dataFolder  string
	tickPeriod  time.Duration
	ignoreCalls bool

	//variables used throughout
	access                  *sync.RWMutex
	started                 bool
	executors               map[string]TaskExecutor
	isPeriodicChecksRunning bool

	store *sett.Sett
	log   Logger
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

//IgnoreCalls Enable ignore calls in cases where you want to ignore
// queuing up jobs (for example in case of unit testing)
// This avoids having to start up  the job queue for unit testing other parts of your system
func (d *JobQueue) IgnoreCalls(ignore bool) {
	d.ignoreCalls = ignore
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
	d.access.Lock()
	defer d.access.Unlock()

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
	// One slot is for periodic checks
	d.jobs = make(chan string, d.NumWorkers-1)
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

/*
readyTable.Update(jid, func(i) error{
job started_at = now
}, lock)


readyTable.Update(jid, func(i) error{
	job completed_at = now
	}, lock)



periodic checks__

readyTable.Filter(func(i){
	started 1 hour back
	  reset
	OR
	not started
	ch <- jid
})

*/
func (d *JobQueue) markJobReady(jid string) error {
	_, err := d.store.Table("jobqueue.ready").Update(jid, func(ij interface{}) error {
		job, ok := ij.(*Job)
		if !ok {
			return fmt.Errorf("JobID %s Wrong type in Job Queue", jid)
		}
		job.MarkReady()
		return nil
	}, true)
	if err != nil {
		return err
	}

	return nil
}

func (d *JobQueue) markJobDropped(jid string) error {
	_, err := d.store.Table("jobqueue.ready").Update(jid, func(ij interface{}) error {
		job := ij.(*Job)
		job.MarkDropped()
		return nil
	}, true)
	if err != nil {
		return err
	}
	return nil
}

func (d *JobQueue) markJobStarted(jid string) (*Job, error) {
	j, err := d.store.Table("jobqueue.ready").Update(jid, func(ij interface{}) error {
		job := ij.(*Job)
		if job.HasStarted() {
			return fmt.Errorf("Job already started")
		}
		job.MarkStarted()
		return nil
	}, true)
	if err != nil {
		return nil, err
	}
	job := j.(*Job)
	return job, nil
}

func (d *JobQueue) markJobCompleted(jid string) error {
	_, err := d.store.Table("jobqueue.ready").Update(jid, func(ij interface{}) error {
		job := ij.(*Job)
		job.MarkCompleted()
		return nil
	}, true)
	if err != nil {
		return err
	}

	return nil
}

func (d *JobQueue) runTask(wid int, jid string) {
	d.log.Logf("Worker %d startrunning job id %s\n", wid, jid)

	job, err := d.markJobStarted(jid)
	if err != nil {
		d.log.Logf("job jid %s didn't start %v", jid, err)
		return
	}

	defer d.markJobCompleted(jid)

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
	d.log.Logf("Worker %d completed %s \n", wid, jid)
}

func (d *JobQueue) runScheduledJobs() {
	scheduledTable := d.store.Table("jobqueue.scheduled")

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
		d.addJobToReadyQueue(job)
		if d.isClosing() {
			return
		}
	}
}
func (d *JobQueue) runDroppedJobs() {
	d.log.Log("running dropped jobs ...")
	readyTable := d.store.Table("jobqueue.ready")
	dropped, err := readyTable.Filter(
		func(k string, j interface{}) bool {
			job := j.(*Job)
			if job.IsDropped() {
				return true
			}
			return false
		})
	if err != nil {
		d.log.Errorf("Error while fetching dropped jobs %v", err)
		return
	}
	for _, jid := range dropped {
		d.log.Logf("signalling dropped job %s ", jid)
		d.signalNewJob(jid)
		if d.isClosing() {
			d.log.Logf("runDroppedJobs: isClosing returning ...")
			//Close signal; will do the scheduled job processing later
			//return home immediately
			return
		}
	}
}
func (d *JobQueue) isPeriodicTaskRunning() bool {
	d.access.RLock()
	defer d.access.RUnlock()
	return d.isPeriodicChecksRunning
}
func (d *JobQueue) lockPeriodicChecks() bool {
	if d.isPeriodicTaskRunning() {
		return false
	}
	d.access.Lock()
	defer d.access.Unlock()
	if d.isPeriodicChecksRunning {
		return false
	}
	d.isPeriodicChecksRunning = true
	return true
}
func (d *JobQueue) unlockPeriodicChecks() {
	d.access.Lock()
	defer d.access.Unlock()
	d.isPeriodicChecksRunning = false

}

func (d *JobQueue) periodicChecks() {
	d.log.Logf("periodicChecks ...")
	if !d.lockPeriodicChecks() {
		d.log.Logf("periodicChecks failed getting lock. returning.")
		return
	}
	defer d.unlockPeriodicChecks()
	if d.isClosing() {
		d.log.Logf("isClosing true returning(pt1) ...")
		return
	}
	if len(d.jobs) >= (cap(d.jobs) - 1) {
		d.log.Logf("periodicChecks- channels are tight. periodic checks postponing")
		return
	}
	d.log.Logf("periodicChecks-  got lock! rock & roll!")
	d.runScheduledJobs()
	if d.isClosing() {
		d.log.Logf("isClosing true returning(pt2) ...")
		return
	}
	d.runDroppedJobs()
	d.log.Logf("periodicChecks completed")
}

func (d *JobQueue) isClosing() bool {
	select {
	case <-d.close:
		return true
	default:
	}
	return false
}
func (d *JobQueue) signalNewJob(jid string) bool {
	d.log.Logf("signalNewJob %s ", jid)
	//This is to keep track when the job signalled ready
	// We will signal again if job is not picked even after some time
	d.markJobReady(jid)
	//check whether already shutting down and close channel is signalled
	select {
	case <-d.close:
		return false
	default:
		d.log.Logf("signalling job %s ", jid)
		select {
		case d.jobs <- jid:
		default:
			//The queue is full. job got dropped
			d.log.Logf("signal dropped %s ", jid)
			d.markJobDropped(jid)
		}

	}
	return true
}

func (d *JobQueue) isStarted() bool {
	d.access.RLock()
	defer d.access.RUnlock()
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
	for _, ticker := range d.recurringTickers {
		ticker.Stop()
	}

	close(d.close)

	d.wg.Wait()
	close(d.jobs)
	d.store.Close()

	d.markStarted(false)
	return nil
}

type JobSnapshot struct {
	ReadyJobs     []*Job
	ScheduledJobs []*Job
}

func (d *JobQueue) GetJobSnapshot() (*JobSnapshot, error) {

	rj, err := d.getAllJobs("jobqueue.ready")
	if err != nil {
		return nil, err
	}
	sj, err := d.getAllJobs("jobqueue.scheduled")
	if err != nil {
		return nil, err
	}

	return &JobSnapshot{rj, sj}, nil
}

func (d *JobQueue) getAllJobs(table string) ([]*Job, error) {
	var ret []*Job
	keys, err := d.store.Table(table).Filter(func(k string, v interface{}) bool {
		return true
	})
	if err != nil {
		return ret, err
	}
	for _, k := range keys {
		j, err := d.store.Table(table).GetStruct(k)
		if err != nil {
			d.log.Errorf("Error getting job item %v", err)
		}
		job := j.(*Job)
		ret = append(ret, job)
	}

	return ret, nil
}

func (d *JobQueue) addJobToReadyQueue(job *Job) error {
	jid, err := d.store.Table("jobqueue.ready").Insert(job)
	if err != nil {
		d.log.Logf("Error adding job to ready queue %v", err)
		return err
	}

	d.log.Logf("Signalling taskID %s Job ID is  %v", job.Task.GetTaskID(), jid)
	d.signalNewJob(jid)
	return nil
}

func (d *JobQueue) runRecurring(job *Job) *time.Ticker {
	ticker := time.NewTicker(job.Repeat)
	go func() {
		d.wg.Add(1)
		defer d.wg.Done()
		for {
			select {
			case <-ticker.C:
				d.addJobToReadyQueue(job)
			case <-d.close:
				return
			}
		}
	}()
	return ticker
}

func (d *JobQueue) scheduleRecurringJob(job *Job) error {
	if job.IsRecurring() {
		ticker := d.runRecurring(job)
		d.recurringTickers = append(d.recurringTickers, ticker)
	}
	return nil
}

func (d *JobQueue) QueueUp(j *Job) error {
	if d.ignoreCalls {
		return nil
	}
	if d.store == nil {
		return fmt.Errorf("Queue is not started up")
	}
	if j.IsRecurring() {
		return d.scheduleRecurringJob(j)
	}

	if j.IsScheduled() {
		_, err := d.store.Table("jobqueue.scheduled").Insert(j)
		if err != nil {
			return err
		}
	} else {
		d.log.Logf("inserting to queue ... ")
		d.addJobToReadyQueue(j)
	}

	return nil
}
