package qup

import (
	"encoding/gob"
	"time"
)

type RunnableTask interface {
}

type Job struct {
	Task       RunnableTask
	Due        time.Time
	Repeat     time.Duration
	RetryCount uint16
}

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
