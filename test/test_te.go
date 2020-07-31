package test

import (
	"sync"
	"testing"
	"time"
)

type TaskStatus struct {
	TimesCalled int
	CreatedAt   time.Time
	ExecutedAt  []time.Time
}

type TestTaskExecutor struct {
	tasks map[string]*TaskStatus
	mutex *sync.Mutex
	t     *testing.T
}

func NewTaskExecutor(t *testing.T) *TestTaskExecutor {
	te := &TestTaskExecutor{t: t}
	te.tasks = make(map[string]*TaskStatus)
	te.mutex = &sync.Mutex{}
	return te
}
func (te *TestTaskExecutor) InitTask(taskID string) {

	te.mutex.Lock()
	var ts TaskStatus
	ts.CreatedAt = time.Now()
	ts.TimesCalled = 0
	te.tasks[taskID] = &ts
	te.mutex.Unlock()
}
func (te *TestTaskExecutor) Execute(t interface{}) error {
	task := t.(*TestTask)
	te.t.Logf("Executing task with ID %s", task.TaskID)

	te.mutex.Lock()
	ts, exists := te.tasks[task.TaskID]
	if exists {
		ts.TimesCalled++
		ts.ExecutedAt = append(ts.ExecutedAt, time.Now())

	}
	te.mutex.Unlock()

	return task.RunTask(te.t)
}

func (te *TestTaskExecutor) PrintStatus() {
	for id, status := range te.tasks {
		d := status.ExecutedAt[0].Sub(status.CreatedAt)
		te.t.Logf("Task %s ran %d times %v time in queue", id, status.TimesCalled, d)
	}
}
func (te *TestTaskExecutor) TaskCount() int {
	return len(te.tasks)
}
func (te *TestTaskExecutor) CheckTaskExecutionCount(expected int) bool {
	for _, status := range te.tasks {
		if status.TimesCalled != expected {
			return false
		}
	}
	return true
}
func (te *TestTaskExecutor) GetTaskCreatedAt(taskID string) time.Time {
	return te.tasks[taskID].CreatedAt
}

func (te *TestTaskExecutor) GetTaskExecutedAt(taskID string) time.Time {
	return te.tasks[taskID].ExecutedAt[0]
}
func (te *TestTaskExecutor) GetExecutionCount(taskID string) int {
	return te.tasks[taskID].TimesCalled
}

func (te *TestTaskExecutor) AllExecutionTimeStamps(taskID string) []time.Time {
	return te.tasks[taskID].ExecutedAt
}
