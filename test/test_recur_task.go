package test

import (
	"testing"
	"time"
)

type TestRecurringTask struct {
	TaskTime time.Duration
	TaskID   string
}

func (task *TestRecurringTask) WithTaskTime(d time.Duration) *TestRecurringTask {
	task.TaskTime = d
	return task
}
func (task *TestRecurringTask) GetTaskID() string {
	return task.TaskID
}

func (task *TestRecurringTask) RunTask(t *testing.T) error {

	t.Logf("Test Recurring Task:%s Running run time %v ", task.TaskID, task.TaskTime)
	//t.logger.Logf("TestRecurringTask Running ... ")

	if task.TaskTime > 0 {
		time.Sleep(task.TaskTime)
	}
	t.Logf("TestRecurringTask Completed.")
	//t.logger.Logf("TestRecurringTask Completed.")
	return nil
}
func CreateTestRecurringTask(taskID string) *TestRecurringTask {

	return &TestRecurringTask{TaskID: taskID}
}
