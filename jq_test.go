package qup_test

import (
	"github.com/prasanthmj/qup"
	"github.com/prasanthmj/qup/test"
	"go.uber.org/goleak"
	"math/rand"
	"os"
	"syreclabs.com/go/faker"
	"testing"
	"time"
)

const testDataFolder = "./data/jobsdb"

func TestMain(m *testing.M) {
	os.RemoveAll(testDataFolder)
	os.Exit(m.Run())
}

type SimpleTask struct{}

func (*SimpleTask) Run() error {

	return nil
}

func TestJobIsDue(t *testing.T) {
	job := qup.NewJob(&SimpleTask{}).After(100 * time.Millisecond)

	if job.IsDue() {
		t.Error("job IsDue when time has not reached")
	}
	job2 := qup.NewJob(&SimpleTask{}).After(100 * time.Millisecond)
	<-time.After(200 * time.Millisecond)
	if !job2.IsDue() {
		t.Error("job IsDue when time has elapsed")
	}
}

func TestWorkerThreadCreation(t *testing.T) {
	defer goleak.VerifyNone(t)

	jq := qup.NewJobQueue().DataFolder(testDataFolder).Logger(t).TickPeriod(200 * time.Millisecond)

	err := jq.Start()
	if err != nil {
		t.Errorf("Can't start jobqueue %v ", err)
		return
	}
	<-time.After(1 * time.Second)
	err = jq.Stop()
	if err != nil {
		t.Errorf("Can't Stop jobqueue %v ", err)
	}
}

func TestWorkerThreadCreationLargerNumber(t *testing.T) {
	defer goleak.VerifyNone(t)

	jq := qup.NewJobQueue().DataFolder(testDataFolder).Logger(t).Workers(100).TickPeriod(200 * time.Millisecond)
	err := jq.Start()
	if err != nil {
		t.Errorf("Can't start jobqueue %v ", err)
		return
	}
	<-time.After(1 * time.Second)
	err = jq.Stop()
	if err != nil {
		t.Errorf("Can't stop jobqueue %v ", err)
		return
	}
}

func TestRunningJob(t *testing.T) {
	defer goleak.VerifyNone(t)
	te := test.NewTaskExecutor(t)

	q := qup.NewJobQueue().DataFolder(testDataFolder).Logger(t)
	q.Register(&test.TestTask{}, te)

	taskID := faker.RandomString(8)
	err := q.Start()
	if err != nil {
		t.Errorf("Can't start jobqueue %v ", err)
		return
	}
	task := test.CreateTestTask(taskID).WithTaskTime(100 * time.Millisecond)
	t.Logf("Task ID %s ", task.TaskID)
	te.InitTask(task.TaskID)

	q.QueueUp(qup.NewJob(task))
	<-time.After(3 * time.Second)
	err = q.Stop()
	if err != nil {
		t.Errorf("Can't stop jobqueue %v ", err)
		return
	}
	//te.PrintStatus()

	if te.TaskCount() != 1 {
		t.Errorf("Expected 1 tasks actual %d", te.TaskCount())
	}
	if !te.CheckTaskExecutionCount(1) {
		t.Errorf("Task Execution count does not match. Expected 1 ")
	}
}

func TestRunningManyJobs(t *testing.T) {
	defer goleak.VerifyNone(t)
	te := test.NewTaskExecutor(t)

	q := qup.NewJobQueue().Workers(10).DataFolder(testDataFolder).Logger(t)
	q.Register(&test.TestTask{}, te)
	err := q.Start()
	if err != nil {
		t.Errorf("Can't start jobqueue %v ", err)
		return
	}
	for i := 0; i < 100; i++ {
		taskID := faker.RandomString(8)
		task := test.CreateTestTask(taskID).WithTaskTime(time.Duration(rand.Intn(100)) * time.Millisecond)
		te.InitTask(task.TaskID)
		q.QueueUp(qup.NewJob(task))
		<-time.After(time.Duration(rand.Intn(100)) * time.Millisecond)
	}

	<-time.After(3 * time.Second)
	err = q.Stop()
	if err != nil {
		t.Errorf("Can't stop jobqueue %v ", err)
		return
	}
	//te.PrintStatus()
	if te.TaskCount() != 100 {
		t.Error("Expected 100 tasks actual ", te.TaskCount())
	}
	if !te.CheckTaskExecutionCount(1) {
		t.Errorf("Task Execution count does not match. Expected 1 ")
	}
}

func TestRunningDelayedJob(t *testing.T) {
	defer goleak.VerifyNone(t)
	te := test.NewTaskExecutor(t)

	q := qup.NewJobQueue().Workers(10).TickPeriod(200 * time.Millisecond).DataFolder(testDataFolder).Logger(t)
	q.Register(&test.TestTask{}, te)
	err := q.Start()
	if err != nil {
		t.Errorf("Can't start jobqueue %v ", err)
		return
	}
	delay := 500 * time.Millisecond
	taskID := faker.RandomString(8)
	task := test.CreateTestTask(taskID).WithTaskTime(time.Duration(rand.Intn(100)) * time.Millisecond)
	te.InitTask(task.TaskID)
	job := qup.NewJob(task).After(delay)
	q.QueueUp(job)

	<-time.After(2 * time.Second)
	err = q.Stop()
	if err != nil {
		t.Errorf("Can't stop jobqueue %v ", err)
		return
	}
	//te.PrintStatus()
	if te.TaskCount() != 1 {
		t.Error("The Delayed Task was not run task count ", te.TaskCount())
	}
	timesCalled := te.GetExecutionCount(taskID)
	if timesCalled != 1 {
		t.Errorf("The Delayed task was not executed as expected Times called %d ", timesCalled)
	}
	createdTime := te.GetTaskCreatedAt(taskID)
	executedTime := te.GetTaskExecutedAt(taskID)

	diff := executedTime.Sub(createdTime)
	if diff < delay {
		t.Errorf("The scheduled job was run before completing the delay! delay %v executed at %v", delay, diff)
	}
	t.Logf("Task Executed after %v ", diff)
}

func TestRunningRecurringJob(t *testing.T) {
	defer goleak.VerifyNone(t)
	te := test.NewTaskExecutor(t)

	q := qup.NewJobQueue().Workers(10).TickPeriod(100 * time.Millisecond).DataFolder(testDataFolder).Logger(t)
	q.Register(&test.TestTask{}, te)
	err := q.Start()
	if err != nil {
		t.Errorf("Can't start jobqueue %v ", err)
		return
	}
	repeats := 200 * time.Millisecond
	taskID := faker.RandomString(8)
	task := test.CreateTestTask(taskID).WithTaskTime(time.Duration(rand.Intn(30)) * time.Millisecond)
	te.InitTask(task.TaskID)
	job := qup.NewJob(task).Every(repeats)
	q.QueueUp(job)

	<-time.After(2 * time.Second)
	err = q.Stop()

	if err != nil {
		t.Errorf("Can't stop jobqueue %v ", err)
		return
	}
	timesCalled := te.GetExecutionCount(taskID)
	if timesCalled < 4 {
		t.Errorf("Didn't run as many times as expected. ran %d times", timesCalled)
	}
	created := te.GetTaskCreatedAt(taskID)
	t.Logf("Times called %d", timesCalled)
	ets := te.AllExecutionTimeStamps(taskID)
	lastCall := created
	for _, ts := range ets {
		diff := ts.Sub(lastCall)
		t.Logf("Ran at %v", diff)
		lastCall = ts
	}
}
