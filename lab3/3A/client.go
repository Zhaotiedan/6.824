package kvraft

import "6.824/labrpc"
import "crypto/rand"
import "math/big"

//保存client信息
type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	seqId int64 //意义：如果这个leader在commit log之后crash了，
	//但是还没响应给client，则client会重发这个command给新的leader，
	//这样会导致这op执行两次，所以每次操作时附加一个唯一的序列号作为标识动作
	//可以避免op被执行两次
	leaderId int //确定哪个服务器是leader，下次直接发送给该服务器
	clientId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

//初始化
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId=nrand() //随机数
	ck.leaderId=mathrand.Intn(len(ck.servers)) //随机选择一个服务器作为leader
	//它会随机选择 ck.servers 列表中的一个索引。
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.seqId++
	args := GetArgs{Key:key, ClientId: ck.clientId, SeqId:ck.seqId}
	serverId := ck.leaderId
	for{
		reply := GetReply{}
		ok := ck.servers[serverId].Call("KVServer.Get", &args, &reply)
		if ok {
			if reply.Err == ErrNoKey {
				ck.leaderId = serverId
				return ""
			}else if reply.Err == OK {
				ck.leaderId = serverId
				return reply.Value
			}else if reply.Err == ErrWrongLeader {
				serverId = (serverId + 1) % len(ck.servers)
				continue
			}
		}
		//节点发送crash等原因
		serverId = (serverId + 1) % len(ck.servers)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.seqId++
	serverId := ck.leaderId
	args := PutAppendArgs{Key:key, Value:value, Op:op, ClientId: ck.clientId, SeqId:ck.seqId}
	for {
		reply := PutAppendReply{}
		ok := ck.servers[serverId].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			if reply.Err ==OK {
				ck.leaderId = serverId
				return
			}else if reply.Err == ErrWrongLeader {
				serverId = (serverId + 1) % len(ck.servers)
				continue
			}
		}
		serverId = (serverId + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
