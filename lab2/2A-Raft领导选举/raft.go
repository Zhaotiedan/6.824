package mr

import (
	"fmt"
	"io/ioutil"
	"strconv"
	"encoding/json"
	"os"
	"time"
	"sort"
)

import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
} 

type SortedKey []KeyValue

//重写len，swap，less，才能排序
func (k SortedKey) Len() int {return len(k)}
func (k SortedKey) Swap(i,j int) {k[i],k[j] = k[j],k[i]}
func (k SortedKey) Less(i,j int) bool {return k[i].Key < k[j].Key}
//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.

//mapf是map函数，它接受两个字符串类型的参数，返回一个 KeyValue 类型的切片（slice）
//reducef是reduce函数，它接受一个字符串和一个字符串的切片作为参数，返回一个字符串
func Worker(mapf func(string, string) []KeyValue,reducef func(string, []string) string) {

	// Your worker implementation here.
	//CallExample()
	keepFlag := true
	for keepFlag{
		task :=GetTask()
		switch task.TaskType {
		case MapTask:
			{	//调用map函数
				DoMapTask(mapf,&task)
				//完成任务，调用rpc在协调者中设置任务已完成
				CallDone(&task)
			}
		case ReduceTask:
			{
				//调用reduce函数
				DoReduceTask(reducef,&task)
				//完成任务，调用rpc在协调者中设置任务已完成
				CallDone(&task)
			}
		case waittingTask:
			{
				fmt.Println("the task are in progress,please wait")
				time.Sleep(time.Second)
			}

		case ExitTask://假任务(ExitTask）的方法退出，当然也可以通过RPC没有获取到task后再退出的方式，可以自己去试试。
			{
				fmt.Println("the task :[",task.TaskId,"] is terminated")
				keepFlag = false
			}
		}
	}
	// uncomment to send the Example RPC to the coordinator.	

}
//获取任务 (参考callexample)
func GetTask() Task{
	args := TaskArgs{}
	reply := Task{}
	ok := call("Coordinator.PollTask",&args,&reply)
	if ok{
		fmt.Println("get task success",reply)
	}else{
		fmt.Println("get task failed")
	}
	return reply
}

func DoMapTask(mapf func(string, string) []KeyValue, response *Task){
	//调用mapf方法处理Map生成一组kv，然后写到temp文件中
	var intermediate []KeyValue//intermediate 是一个 KeyValue 类型的切片。KeyValue 是一个类型

	//1.从文件中读取内容
	filename := response.FileSlice[0]//获取文件名
	file,err := os.Open(filename)//打开文件
	if err != nil{
		log.Fatalf("cannot open %v",filename)
	}
	//通过io工具包获取文件的内容content，作为mapf的参数
	content,err := ioutil.ReadAll(file)
	if err != nil{
		log.Fatalf("cannot read %v",filename)
	}
	file.Close()

	//2.调用mapf函数，将文件内容传入，返回一个KeyValue类型的切片数组
	intermediate = mapf(filename,string(content))

	//3.遍历结构体，创建一个长ReducerNum的二维切片，用于保存切片结果
	rn := response.ReducerNum
	HashedKV := make([][]KeyValue,rn)

	//4.遍历kv，将切片结果写入hashKV中，ihash(kv.Key)%rn相同的kv交给同一个reduce处理
	//ihash(kv.Key)%rn：将ihash值映射到0-rn之间
	for _, kv := range intermediate {
		HashedKV[ihash(kv.Key)%rn] = append(HashedKV[ihash(kv.Key)%rn],kv)//将 kv 添加到对应的 KeyValue 切片中。
	}

	//5.将HashedKV中的内容写入临时文件中
	for i :=0; i < rn; i++{
		oname := "mr-tmp-" + strconv.Itoa(response.TaskId) + "-" + strconv.Itoa(i)//taskid号任务，被分成rn个文件
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)//json编码器
		for _, kv := range HashedKV[i]{
			enc.Encode(kv)//将kv以json格式写入ofile文件
			if err != nil{
				return
			}
		}
		ofile.Close()
	}
}

func DoReduceTask(reducef func(string, []string) string, response *Task){
	//对之前的tmp文件进行洗牌（shuffle），得到一组排序好的kv数组,并根据重排序好kv数组重定向输出文件。
	reduceFileNum := response.TaskId
	intermediate := shuffle(response.FileSlice)
	dir, _ := os.Getwd()//获取当前工作目录
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")//获取mr-tmp-开头的文件
	if err != nil{
		log.Fatalf("fail to create temp file",err)
	}
	i := 0
	//根据重排序好kv数组重定向输出文件。
	for i < len(intermediate){
		j := i + 1
		//跳过key值一样的，将value值累加
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key{
			j++
		}
		//保存当前key的所有value值,存于values切片中
		var values []string
		for k := i; k < j; k++{
			values = append(values,intermediate[k].Value)
		}
		//调用reducef函数，将key和value值传入，返回一个字符串
		output := reducef(intermediate[i].Key,values)
		fmt.Fprintf(tempFile,"%v %v\n",intermediate[i].Key,output)
		i = j
	}
	tempFile.Close()
	fn := "mr-out-" + strconv.Itoa(reduceFileNum)
	os.Rename(tempFile.Name(),fn)
}
//洗牌方法：得到一组排序好的kv数组
func shuffle(files []string) []KeyValue{
	var kva []KeyValue
	//将files内容保存到kva
	for _, filepath := range files{
		file, _ := os.Open(filepath)
		dec := json.NewDecoder(file)
		for{
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil{
				break
			}
			kva = append(kva,kv)//kv 是被添加到 kva 切片的末尾的元素。然后，append 函数返回的新切片被赋值给 kva。
		}
		file.Close()
	}
	//排序kva，先将kva转化为sort.interface类型，才能被排序，再调用sort.Sort方法
	sort.Sort(SortedKey(kva))
	return kva
}

func CallDone(f* Task) Task{
	args := f
	reply := Task{}

	ok := call("Coordinator.MarkFinished",&args,&reply)
	if ok{
		fmt.Println("worker finished:",reply)
	}else{
		fmt.Printf("call failed\n")
	}
	return reply
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
