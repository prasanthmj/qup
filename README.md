# qUP 
(called Queue Up)  

qUP is a background task processor with persistence support. It uses BadgerDb for persistence.  

In your go app, you can run background tasks using go routines easily. 
However, suppose you want to restart the app. How do you know how many tasks are pending completion, how to restart the pending tasks back after the app restarts?  

Using qUp, you can add tasks to a pool that are executed concurrently. When you restart the app, the task queue is restored from persistent storage. The 
tasks continue execution.  

## How to use

First you have to create an instance of JobQueue 
```go
jq := qup.NewJobQueue().Workers(100)
//Bring up the worker pool
jq.Start()
...
...
//On shutting down the app
jq.Stop()
```

## Task Executors 

You have to register task executors for each type of tasks
```go

jq.Register(&OrderEmail{}, sender)

```
Task executor must implement an interface `Execute`
```go
func (this *OrderSender) Execute(t interface{}) error {
    orderEmail,ok :=  t.(*OrderEmail)
    .....
    
}
```

Now you can add Jobs to the queue as they arrive

### Make the task run after a few minutes
This is useful when you want to do  task after associated processes are completed. For example, payment processing updates are completed.
```go
//Add a delayed job
j := qup.NewJob(&OrderEmail{orderID}).After(5 * time.Minute)
jq.QueueUp(j)
```
Suppose you added a task to be ran after 5 minutes. After 2 minutes the app was shutdown. When the app comes up again, the task will be executed.
This wouldn't work if say, had you implemented the task as a go routine using `time.AfterFunc()`

### Add Recurring tasks
```go
j := qup.NewJob(&CleanupTables{}).Every(1 * time.Hour)
jq.QueueUp(j)

```
qUp will check for duplicates; so even if you add the same job multiple times, only the last job will be added for recurrance.

### Run the task immediately
```go
j := qup.NewJob(&SendNotification{})
jq.QueueUp(j)
```
