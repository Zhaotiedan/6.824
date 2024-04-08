package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//
import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

//work从coordinator获取task的结构体
type Task struct{
	TaskType TaskType //任务类型：map或者reduce
	TaskId int //任务id
	ReducerNum int //转入的reducer的数量，用于hash
	FileSlice  []string // 输入文件的切片，map对应一个文件，reduce对应多个hash值相同的temp文件
}
type TaskType int //任务的父类型
type State int //state任务状态
type Phase int //分配任务阶段的父类型


//TaskArgs rpc应该传入的参数，实际什么都不传，因为只是worker获取一个任务
type TaskArgs struct{}

//枚举任务类型
const(
	MapTask TaskType = iota//定义一个 TaskType 类型的常量 MapTask，并将其初始化为 0（假设这是 const 声明块中的第一个常量）。
	ReduceTask //==1
	waittingTask //==2 代表该任务已经分发，但是任务还没完成，阶段不改变
	ExitTask     // exit
)
//任务状态类型
const(
	Working State = iota //工作阶段
	Waiting //等待执行
	Done //已完成
)
//阶段类型
const(
	MapPhase Phase = iota //分发MapTask
	ReducePhase //分发ReduceTask
	AllDone //完成阶段
)


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
