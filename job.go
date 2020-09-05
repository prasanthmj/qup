package qup

import (
	"encoding/gob"
	"reflect"
	"time"
)

//RunnableTask contains the data for the task (does not directly run the task)
// GetTaskID() is uniquely identifying this task
type RunnableTask interface {
	GetTaskID() string
}

//Job is a wrapper around the task data and carries
// The task progress information.
type Job struct {
	Task        RunnableTask
	Due         time.Time
	Repeat      time.Duration
	RetryCount  uint16
	ReadyAt     time.Time
	StartedAt   time.Time
	CompletedAt time.Time
}

const (
	READY_WAIT_TIME = 100 * time.Millisecond
)

func NewJob(t RunnableTask) *Job {
	gob.Register(&Job{})
	return &Job{Task: t}
}
func (j *Job) IsDue() bool {
	if j.Due.IsZero() {
		return false
	}
	res := time.Now().Sub(j.Due).Milliseconds()
	if res >= 0 {
		return true
	}
	return false
}
func (j *Job) After(d time.Duration) *Job {
	j.Due = time.Now().Add(d)
	return j
}
func (j *Job) Retries(r uint16) *Job {
	j.RetryCount = r
	return j
}

func (j *Job) Every(d time.Duration) *Job {
	j.Repeat = d
	return j
}

func (j *Job) IsRecurring() bool {
	if j.Repeat > 0 {
		return true
	}
	return false
}

func (j *Job) ScheduleNextDue() {
	if j.IsRecurring() {
		j.After(j.Repeat)
	}
}

func (j *Job) IsScheduled() bool {
	if j.Due.IsZero() {
		return false
	}
	return true
}

func (j *Job) AreYouSame(other *Job) bool {
	myname := reflect.TypeOf(j.Task).String()
	othername := reflect.TypeOf(other.Task).String()

	return (myname == othername)
}

func (j *Job) HasStarted() bool {
	if j.StartedAt.IsZero() {
		return false
	}
	return true
}

func (j *Job) MarkStarted() {
	j.StartedAt = time.Now()
}

func (j *Job) IsCompleted() bool {
	if j.CompletedAt.IsZero() {
		return false
	}
	return true
}

func (j *Job) MarkCompleted() {
	j.CompletedAt = time.Now()
}

func (j *Job) MarkReady() {
	j.ReadyAt = time.Now()
}

func (j *Job) MarkDropped() {
	var zt time.Time
	j.ReadyAt = zt
}

func (j *Job) IsDropped() bool {
	if j.ReadyAt.IsZero() {
		return true
	}
	return false
}
