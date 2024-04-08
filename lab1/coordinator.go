package mr

import(
	"fmt"
	"sync"
	"io/ioutil"
	"strconv"
	"strings"
	"time"
)
import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

// 定义为全局锁，worker之间访问coordinator时加锁
var (
	mu sync.Mutex
)

type Coordinator struct {
	// Your definitions here.
	TaskId int //决定当前要分配的任务id
	ReducerNum int //决定要多少个reducer
	DistPhase Phase //当前整个框架处于什么任务阶段
	MapTaskChannel chan *Task //执行map任务的channel：通道中元素类型为指向Task的指针
	ReduceTaskChannel chan *Task //执行reduce任务的channel
	taskMetaHolder TaskMetaHolder //存放task元数据的map
	files []string //输入文件
}

//保存任务的元数据
type TaskMetaHolder struct{
	MetaMap map[int]*TaskMetaInfo //定义一个map叫MetaMap。key：int，value：TaskMetaInfo*
}
//任务的元数据
type TaskMetaInfo struct{
	state State//任务状态
	TaskAdr *Task //任务地址，为的是这个任务从通道中取出来后，还能通过地址标记这个任务已经完成
	StartTime time.Time //任务开始时间,为crash做准备
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//

//主函数mr调用，所有任务完成，mr通过此方法退出
func (c *Coordinator) Done() bool {
	mu.Lock()
	defer mu.Unlock()
	//ret := false

	// Your code here.
	if c.DistPhase == AllDone{
		fmt.Printf("all task is done,the Coordinator is exiting")
		return true
	}else{
		return false
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	// Your code here.
	//创建协调者
	c := Coordinator{
		files: files,
		ReducerNum: nReduce,
		DistPhase: MapPhase,
		MapTaskChannel: make(chan *Task, len(files)),
		ReduceTaskChannel: make(chan *Task, nReduce),
		taskMetaHolder: TaskMetaHolder{
			MetaMap: make(map[int]*TaskMetaInfo, len(files) + nReduce),// 任务的总数应该是files + Reducer的数量
		},
  
	}
	//创建map任务
	c.makeMapTasks(files)
	c.server()
	//go c.CrashDetector()
	return &c
}

//创建map任务
//将Map任务放到Map管道中，taskMetaInfo放到TaskMetaHolder中
//(c* Coordinator) 是方法的接收者。这意味着 makeMapTasks 方法是绑定到 coordinator 类型的指针上的。在方法内部，你可以通过 c 变量来访问和修改 coordinator 对象的字段。
func (c* Coordinator)makeMapTasks(files []string){
	for _, v := range files{
		id := c.generateTaskId()
		task := Task{
			TaskType: MapTask,
			TaskId: id,
			ReducerNum: c.ReducerNum,
			FileSlice: []string{v},//切片，初始化为v
		}
		//保存任务初始状态
		taskMetaInfo := TaskMetaInfo{
			state: Waiting,//任务初始状态为等待被执行
			TaskAdr: &task,//任务地址
		}
		//将任务元数据放入TaskMetaHolder
		c.taskMetaHolder.acceptMeta(&taskMetaInfo)

		//打印任务
		fmt.Println("make Map Task:",&task)
		//将map任务放入map管道
		c.MapTaskChannel <- &task
	}
}


//生成id的方法(主键自增方式)
func (c *Coordinator)generateTaskId()int {
	res := c.TaskId
	c.TaskId++
	return res;
}

//将taskMetaInfo放入TaskMetaHolder
func(t* TaskMetaHolder) acceptMeta(TaskInfo *TaskMetaInfo )bool{
	taskId := TaskInfo.TaskAdr.TaskId
	meta, _ := t.MetaMap[taskId]
	//如果当前任务元数据已经存储于TaskMetaHolder
	if(meta != nil){
		fmt.Println("meta contains task:",taskId)
		return false
	}else{
		t.MetaMap[taskId] = TaskInfo
	}
	return true
}
//分发任务(rpc)，给worker
//map:将map任务管道中的任务取出，如果取不出来，说明任务已经取尽，那么此时任务要么就已经完成，要么就是正在进行。判断任务map任务是否先完成，如果完成那么应该进入下一个任务处理阶段（ReducePhase）
func(c* Coordinator) PollTask(args *TaskArgs, reply *Task)error{
	//分发任务上锁，防止多个worker竞争，同时使用defer回退解锁
	mu.Lock()
	defer mu.Unlock()

	//判断任务存储类型
	switch c.DistPhase {
	case MapPhase:
		{
			//chan中还有map任务
			if len(c.MapTaskChannel) > 0 {
				fmt.Printf("poll-Map-taskid[ %d ]\n", reply.TaskId)
				*reply = *<-c.MapTaskChannel//注意改变的是reply指向的值，而不是reply本身，所以要写*reply =*...。另外<-c.MapTaskChannel才是从通道接收到的值，所以*<-c.MapTaskChannel
				//判断当前任务是否已经在working状态
				if !c.taskMetaHolder.JudgeState(reply.TaskId){
					fmt.Printf("map task %d is working\n",reply.TaskId)
				}
			}else{//如果取不出来，说明任务已经取尽，那么此时任务要么就已经完成，要么就是正在进行
				// 所有map任务被分发完了但是又没完成，此时就将任务设为Waitting
				reply.TaskType = waittingTask
				//检测所有map任务是否完成，若全部完成，则整个框架切换下一个阶段
				if c.taskMetaHolder.checkTaskDone(){
					fmt.Println("all map task is done")
					c.toNextPhase()
				}
				return nil
			}
		}
	case ReducePhase:
		{
			if len(c.ReduceTaskChannel) > 0 {
				*reply = * <-c.ReduceTaskChannel
				//判断当前task是否已经在working状态
				if !c.taskMetaHolder.JudgeState(reply.TaskId){
					fmt.Printf("task %d is working\n",reply.TaskId)
				}
			}else{
				//reduce任务已经分发完了但是又没完成，此时就将任务设为Waitting
				reply.TaskType = waittingTask
				//检测所有reduce任务是否完成，若全部完成，则整个框架切换下一个阶段
				if c.taskMetaHolder.checkTaskDone(){
					c.toNextPhase()
				}
				return nil
			}
		}
	case AllDone:
		{
			reply.TaskType = ExitTask
		}
	default:
		{
			panic("phase error")
		}
	}
	return nil//代表没有错误
}
//分配任务时转换阶段
func(c* Coordinator) toNextPhase(){
	//1.此时为map阶段，转换为reduce阶段
	if c.DistPhase == MapPhase{
		//reduce
		fmt.Printf("to reduce")
		c.makeReduceTasks()
		c.DistPhase = ReducePhase
	}else if c.DistPhase == ReducePhase{//2.此时为reduce阶段，将reduce阶段转换为allDone阶段
		//all done
		c.DistPhase = AllDone
	}
}
//判断任务是否完成
func(t* TaskMetaHolder)checkTaskDone()bool{
	fmt.Println("check task done")
	var(
		mapDoneNum = 0
		mapUnDoneNum = 0
		reduceDoneNum = 0
		reduceUnDoneNum = 0
	)
	//遍历存储task元数据信息的MetaMap 统计已完成的map和reduce和未完成的map和reduce
	for _, v := range t.MetaMap{
		//map
		if v.TaskAdr.TaskType == MapTask{
			fmt.Printf("map task %d state is %d\n",v.TaskAdr.TaskId,v.state)
			if v.state == Done{
				mapDoneNum++
			}else{
				mapUnDoneNum++
			}
		}else if v.TaskAdr.TaskType == ReduceTask{//reduce
			if v.state == Done{
				reduceDoneNum++
			}else{
				reduceUnDoneNum++
			}
		}
	}
	fmt.Printf("mapDoneNum:%d,mapUnDoneNum:%d,reduceDoneNum:%d,reduceUnDoneNum:%d\n",mapDoneNum,mapUnDoneNum,reduceDoneNum,reduceUnDoneNum)
	//根据已完成和未完成的任务数判断:
	//如果某一个map或者reduce全部做完了，代表需要切换下一阶段，返回true
	//1.map全部完成，切换,到reduce阶段
	if (mapDoneNum > 0 && mapUnDoneNum == 0) && (reduceDoneNum == 0 && reduceUnDoneNum == 0){
		fmt.Println("map is all done")
		return true
	}else{
		//2.reduce全部完成，切换,到allDone阶段
		if reduceDoneNum > 0 && reduceUnDoneNum == 0{
			return true
		}
	}
	return false
}

// 判断给定任务是否在工作，没有在工作，则修改其目前任务信息状态
func (t* TaskMetaHolder)JudgeState(taskId int) bool {
	taskInfo, ok := t.MetaMap[taskId]
	//task元数据不存在于MetaMap中，或者当前task不在wating(等待执行)状态
	if !ok || taskInfo.state != Waiting{
		return false
	}
	//修改task元数据状态为working<并更新任务开始时间
	taskInfo.state = Working
	taskInfo.StartTime = time.Now()
	return true
}

//woker完成后，将任务标记为已完成 (rpc)
func (c* Coordinator)MarkFinished(args *Task, reply *Task)error{
	mu.Lock()
	defer mu.Unlock()
	switch args.TaskType {
	case MapTask:
		meta,ok := c.taskMetaHolder.MetaMap[args.TaskId]	
		if ok && meta.state == Working{
			meta.state = Done
			fmt.Printf("map task %d is finished\n",args.TaskId)
		}else{
			fmt.Printf("map task %d is finished,aready\n",args.TaskId)
		}
		break
	case ReduceTask:
		meta,ok := c.taskMetaHolder.MetaMap[args.TaskId]
		if ok && meta.state == Working{
			meta.state = Done
			fmt.Printf("reduce task %d is finished\n",args.TaskId)
		}else{
			fmt.Printf("reduce task %d is finished,aready\n",args.TaskId)
		}
		break
	default:
		panic("task type error")
	}
	return nil
}

//创建reduce任务
func (c* Coordinator)makeReduceTasks(){
	for i:=0; i<c.ReducerNum; i++{
		id := c.generateTaskId()
		task := Task{
			TaskType: ReduceTask,
			TaskId: id,
			FileSlice: selectReduceName(i),
		}
		//设置任务初始状态
		taskMetaInfo := TaskMetaInfo{
			state: Waiting,
			TaskAdr: &task,
		}
		//将任务元数据放入TaskMetaHolder
		c.taskMetaHolder.acceptMeta(&taskMetaInfo)
		//打印任务
		fmt.Println("make Reduce Task:",&task)
		//将reduce任务放入reduce管道
		c.ReduceTaskChannel <- &task
	}
}

//挑选reduce阶段的文件切片
func selectReduceName(reduceNum int) []string{
	var s []string
	path, _ := os.Getwd()//获取当前工作目录的路径。
	files, _ := ioutil.ReadDir(path)// 读取当前工作目录中的所有文件和目录，返回一个 os.FileInfo 类型的切片
	for _, file := range files{
		//匹配对应的reduce文件
		//HasPrefix和HasSuffix检测字符串是否以指定的字符串开头和结尾
		if strings.HasPrefix(file.Name(), "mr-tmp") && strings.HasSuffix(file.Name(), strconv.Itoa(reduceNum)){
			s = append(s, file.Name())
		}
	}
	return s
}

//探测crash协程:将超过10s的任务都放回chan中，等待任务重新读取。
func (c* Coordinator)CrashDetector(){
	for{
		//放松的对锁的获取（sleep 2s)有点像时间片轮转
		time.Sleep(time.Second * 2)
		mu.Lock()
		//如果所有任务都已经完成，那么就不需要探测了
		if c.DistPhase == AllDone{
			mu.Unlock()
			break
		}
		for _,v := range c.taskMetaHolder.MetaMap{
			//如果任务处于working状态，且已经超过10s，那么就认为任务已经crash，将任务放回chan中
			if v.state == Working && time.Since(v.StartTime) > 9*time.Second{
				fmt.Printf("task %d is crash, aready take [%d] s\n",v.TaskAdr.TaskId,time.Since(v.StartTime))
				//判断任务类型，map放入map的chan，reduce放入reduce的chan
				switch v.TaskAdr.TaskType {
				case MapTask:
					c.MapTaskChannel <- v.TaskAdr
					v.state = Waiting
				case ReduceTask:
					c.ReduceTaskChannel <- v.TaskAdr
					v.state = Waiting
				}
			}	
		}
	}
	mu.Unlock()
}
