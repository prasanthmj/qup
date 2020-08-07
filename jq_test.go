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

func (*SimpleTask) GetTaskID() string {

	return "simple-taskid"
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

	js, err := q.GetJobSnapshot()
	if err != nil {
		t.Errorf("Error getting job snapshot %v ", err)
	} else {
		if len(js.ReadyJobs) < 1 {
			t.Errorf("No completed jobs in queue")
		} else if !js.ReadyJobs[0].IsCompleted() {
			t.Errorf("The job was not completed")
		} else {
			t.Logf("num jobs in ready queue %d ", len(js.ReadyJobs))
			ready := js.ReadyJobs[0].CompletedAt.Sub(js.ReadyJobs[0].ReadyAt)
			started := js.ReadyJobs[0].CompletedAt.Sub(js.ReadyJobs[0].StartedAt)
			t.Logf("ready %v started %v ", ready, started)
			t.Logf("Scheduled jobs %d ", len(js.ScheduledJobs))

		}
	}

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

func addDelayedTaskToQueue(t *testing.T, te *test.TestTaskExecutor, taskID string) {
	q := qup.NewJobQueue().Workers(10).TickPeriod(200 * time.Millisecond).DataFolder(testDataFolder).Logger(t)
	q.Register(&test.TestTask{}, te)
	err := q.Start()
	if err != nil {
		t.Errorf("Can't start jobqueue %v ", err)
		return
	}
	delay := 2 * time.Second

	task := test.CreateTestTask(taskID).WithTaskTime(time.Duration(rand.Intn(100)) * time.Millisecond)
	te.InitTask(task.TaskID)
	job := qup.NewJob(task).After(delay)
	q.QueueUp(job)
	t.Logf("Added the Job. Waiting for some time...")
	<-time.After(1 * time.Second)
	t.Logf("Stopping the queue...")
	err = q.Stop()
	if err != nil {
		t.Errorf("Can't stop jobqueue %v ", err)
		return
	}
}
func TestRunningDelayedJobAfterRestart(t *testing.T) {
	defer goleak.VerifyNone(t)
	te := test.NewTaskExecutor(t)
	taskID := faker.RandomString(8)
	addDelayedTaskToQueue(t, te, taskID)
	t.Logf("Created a queue and stopped it. Wait for some time ...")
	<-time.After(1 * time.Second)
	t.Logf("Starting the queue now")
	//Created the task to run after 2 second and stopped the queue.
	//Let us now start the queue again
	q := qup.NewJobQueue().Workers(10).TickPeriod(200 * time.Millisecond).DataFolder(testDataFolder).Logger(t)
	q.Register(&test.TestTask{}, te)
	err := q.Start()
	if err != nil {
		t.Errorf("Can't start jobqueue %v ", err)
		return
	}
	defer q.Stop()
	t.Logf("Waiting for the queue ...")
	<-time.After(2 * time.Second)

	t.Logf("Checking for task execution status ...")
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

	t.Logf("Task Executed after %v ", diff)
}

func TestOverflowRegularTasks(t *testing.T) {
	const TasksCount = 1000
	defer goleak.VerifyNone(t)
	te := test.NewTaskExecutor(t)
	q := qup.NewJobQueue().Workers(10).TickPeriod(50 * time.Millisecond).DataFolder(testDataFolder).Logger(t)
	q.Register(&test.TestTask{}, te)
	err := q.Start()
	if err != nil {
		t.Errorf("Can't start jobqueue %v ", err)
		return
	}
	for i := 0; i < TasksCount; i++ {
		addTaskToQueue(t, q, te, 0)
	}
	<-time.After(300 * time.Millisecond)

	err = q.Stop()
	if err != nil {
		t.Errorf("Can't stop jobqueue %v ", err)
		return
	}
	if !te.AssertAllTasksExecutedExactlyOnce() {
		t.Errorf("Not all tasks executed as expected")
	}
	tec := te.GetTaskExecutionCount()
	if tec != TasksCount {
		t.Errorf("Task execution count expected %d actual %d", TasksCount, tec)
	}
	//te.PrintStatus()
}

func addTaskToQueue(t *testing.T, q *qup.JobQueue, te *test.TestTaskExecutor, repeats time.Duration) {
	taskID := faker.RandomString(8)
	task := test.CreateTestTask(taskID).WithTaskTime(time.Duration(rand.Intn(10)) * time.Millisecond)
	te.InitTask(task.TaskID)
	job := qup.NewJob(task)
	if repeats > 0 {
		job.Every(repeats)
	}
	err := q.QueueUp(job)
	if err != nil {
		t.Errorf("Error queuing up job %v", err)
	}
}

func TestRunningRecurringAndOtherJobs(t *testing.T) {
	defer goleak.VerifyNone(t)
	te := test.NewTaskExecutor(t)

	q := qup.NewJobQueue().Workers(10).TickPeriod(50 * time.Millisecond).DataFolder(testDataFolder).Logger(t)
	q.Register(&test.TestTask{}, te)
	q.Register(&test.TestRecurringTask{}, te)

	err := q.Start()
	if err != nil {
		t.Errorf("Can't start jobqueue %v ", err)
		return
	}

	for j := 0; j < 10; j++ {
		repeats := 60 + time.Duration(rand.Intn(80))*time.Millisecond
		addTaskToQueue(t, q, te, repeats)
	}

	for i := 0; i < 1000; i++ {
		addTaskToQueue(t, q, te, 0)
	}

	<-time.After(5 * time.Second)

	t.Log("--------------Stopping now ------------------------------")
	err = q.Stop()
	if err != nil {
		t.Errorf("Can't stop jobqueue %v ", err)
		return
	}
	if !te.AssertAllTasksExecutedAtleastOnce() {
		t.Errorf("Not all tasks executed as expected")
	}
	tec := te.GetTaskExecutionCount()
	t.Logf("Total Executions %d", tec)
	te.PrintStatus()

}
