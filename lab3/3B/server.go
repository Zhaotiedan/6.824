package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
	"fmt"
	"bytes"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

//op结构体的设计要能接受上层client发来的参数，并且能够转接的raft服务层回来的command。
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	SeqId int64
	Key string
	Value string
	ClientId int64
	Index int //raft服务层传来的index
	OpType string 
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	seqMap map[int64]int64 //clientId -> seqId 确保每个seq只执行一次
	waitChMap map[int]chan Op //index -> chan 下层Raft服务的appCh
	kvPersist map[string]string //key -> value 存储持久化的kv键值对
}

func (kv* KVServer) getWaitCh(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	ch,exist  := kv.waitChMap[index]
	if !exist {
		kv.waitChMap[index] = make(chan Op, 1)
		ch = kv.waitChMap[index]
	}
	return ch

}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	//封装op传递到下层start
	op := Op{OpType: "Get", Key: args.Key, SeqId: args.SeqId, ClientId: arg.ClientId, }
	
	lastIndex, _, _ := kv.rf.Start(op)
	ch := kv.getWaitCh(lastIndex) //获取raftStart对应下标的缓冲chan

	defer func() {
		kv.mu.Lock()
		delete(kv.waitChMap, op.Index)//从kv.waitChMap中删除键为op.Index的元素
		kv.mu.Unlock()
	}()

	//设置超时ticker
	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()

	select{
	case replyOp := <-ch: //将通道中数据给replyOp
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			reply.Err = ErrWrongLeader
			return
		}else{
			reply.Err = OK
			kv.mu.Lock()
			reply.Value = kv.kvPersist[args.Key]
			kv.mu.Unlock()
			return
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	//封装op传递到下层start
	op := Op{OpType: args.Op, Key: args.Key, Value: args.Value, SeqId: args.SeqId, ClientId: args.ClientId}
	lastIndex,_,_ := kv.rf.Start(op)
	ch := kv.getWaitCh(lastIndex)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChMap, op.Index)
		kv.mu.Unlock()
	}()

	//设置超时ticker
	timer := time.NewTicker(100 * time.Millisecond)
	select{
	case replyOp := <-ch:
		//通过clientid，seqid指定唯一操作序列
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			reply.Err = ErrWrongLeader
			return
		}else{
			reply.Err = OK
			return
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}
	defer timer.Stop()

}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.seqMap = make(map[int64]int64)
	kv.waitChMap = make(map[int]chan Op)
	kv.kvPersist = make(map[string]string)

	kv.lastIncludedIndex = -1

	//可能会crash重连
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.DecodeSnapShot(snapshot)
	}

	go kv.applyMsgHandler()//开启转接loop

	return kv
}

//转接信息的loop：将applyMsg转接成op再存到waitCh中
func (kv *KVServer) applyMsgHandler() {
	for{
		if kv.killed() {
			return
		}
		select{
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				//传来的信息快照已经存储过了
				if msg.CommandIndex <= kv.lastIncludedIndex {
					return
				}
			}
			index := msg.CommandIndex
			op := msg.Command.(Op)
			//判断是否是重复操作
			if !kv.ifDuplicate(op.ClientId, op.SeqId) {
				kv.mu.Lock()
				switch op.OpType {
				case "Put":
					kv.kvPersist[op.Key] = op.Value
				case "Append":
					kv.kvPersist[op.Key] += op.Value
				}
				kv.seqMap[op.ClientId] = op.SeqId
				kv.mu.Unlock()
			}
			//如果需要snapshot，且超过其stateSize
			if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
				snapshot := kv.PersistSnapShot()
				kv.rf.Snapshot(index, snapshot)
			}

			kv.getWaitCh(index) <- op
		}
		if msg.SnapshotValid {
			kv.mu.Lock()
			//判断此时有无竞争
			if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
				//读取快照的数据
				kv.DecodeSnapShot(msg.Snapshot)
				kv.lastIncludedIndex = msg.SnapshotIndex
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) ifDuplicate(clientId int64, seqId int64) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	lastSeqId, exist := kv.seqMap[clientId]
	if !exist {
		return false
	}
	return seqId <= lastSeqId //如果小于等于上次的seqId则说明是重复操作
}

//序列化对应持久化的数据
func (kv *KVServer) PersistSnapShot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.kvPersist)
	e.Encode(kv.seqMap)

	data := w.Bytes()
	return data
}

//反序列化
func (kv *KVServer) DecodeSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var kvPersist map[string]string
	var seqMap map[int64]int64

	if d.Decode(&kvPersist) == nil && d.Decode(&seqMap) == nil {
		kv.kvPersist = kvPersist
		kv.seqMap = seqMap
	} else {
		fmt.Printf("[Server(%v)] Failed to decode snapshot",kv.me)
	}
}

