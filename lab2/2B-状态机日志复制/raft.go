package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
	"fmt"
	"time"
	"math/rand"
)


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
// --------------------------------------------------------类型/结构体定义----------------------------------------------------
//status
//appendEntries
//requestVote
type Status int //节点的角色
type VoteState int //投票状态
type AppendEntriesState int //追加日志的状态 2a 2b
type LogEntry struct {
	Term int //日志条目的任期号
	Command interface{} //日志条目的指令
}

var HeartBeatTimeout = 120*time.Millisecond//全局心跳时间

//枚举节点角色类型：群众，竞选者，领导者
const (
	Follower Status = iota
	Candidate
	Leader
)
//枚举投票的状态：投票正常，raft节点终止，投票过期，本term已投过票
const (
	Normal VoteState = iota
	Killed
	Expire 
	Voted
)
//枚举追加日志的状态：追加成功，追加失败
const (
	AppNormal AppendEntriesState = iota //追加正常
	AppOutOfDate //追加过时
	AppKilled //raft程序终止
	// AppRepeat //
	AppCommited //追加的日志已提交 2b
	Mismatch //追加不匹配,fllower落后 2b
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state: 保护peers状态共享访问的锁
	peers     []*labrpc.ClientEnd // RPC end points of all peers 用rpc通信的"节点" 
	persister *Persister          // Object to hold this peer's persisted state 保存持久化状态的地方
	me        int                 // this peer's index into peers[] peer的序号
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//1.所有服务器上持久存在的
	currentTerm int //记录当前的任期
	votedFor int //记录该节点把票投给了谁
	logs []LogEntry //日志条目数组；包含状态机要执行的指令集，以及收到领导的任期号

	//2.所有服务器上经常修改的：
	//正常情况下commitIndex = lastApplied，但是如果有一个新的提交但是还没有被应用，那么commitInde > lastApplied
	commitIndex int //已知的最大的已经被提交的日志条目的索引值。1.leader与和客户端 2.followers和leader
	lastApplied int //最后被应用到状态机的日志条目索引值

	//3.leader拥有的，用于管理他的followers
	//nextIndex和matchIndex初始化长度为len(peers)，因为leader对每个follower都记录一个nextIndex[i]和matchIndex[i]
	//nextIndex指下一个appendEntries从哪里开始
	//matchIndex指已知的某个follower的log和leader的log最大匹配到第几个index，已经apply
	nextIndex []int //对于每一个服务器，需要发送给他的下一个日志条目的索引值，注意nextindex从1开始
	matchIndex []int //对于每一个服务器，已经复制给他的日志的最高索引值

	//自己追加的
	status Status //当前节点是什么角色(状态)
	overtime time.Duration //超时时间
	timer *time.Ticker //每个节点的计时器
	// votedNums int //投票数量

	//2B
	applyChan chan ApplyMsg //存放日志的地方
}
// --------------------------------------------------------RPC参数部分----------------------------------------------------


//RequestVote RPC
//注意所有的index都是从1开始的，所以访问rf.log这个数组的时候要减1
type RequestVoteArgs struct {
	Term int //候选人的任期号
	CandidateId int //请求选票的候选人的id
	LastLogIndex int //候选人的最后日志条目的索引
	LastLogTerm int //候选人最后日志条目的任期号
}
type RequestVoteReply struct {
	Term int //投票者的term，如果竞选者比自己还低，就让他更新为自己的term
	VoteGranted bool //是否投票给候选人了
	VoteState VoteState //投票状态
}

// AppendEntries RPC 心跳连接建立,包括2b的leader复制日志条目
type AppendEntriesArgs struct {
	Term int //领导人的任期号
	LeaderId int //领导人的id
	PrevLogIndex int //上一次日志存储的下标，紧接在新条目之前的日志条目的索引。用于匹配的日志位置是否合适， 应该是针对于follower的args初始化为：rf.nextIndex[i] - 1
	PrevLogTerm int //上一次日志的任期。用于匹配的日志任期是否合适，是否有冲突
	Entries []LogEntry //预计要存储的日志（如果为空则是心跳连接）
	LeaderCommit int //leader的最后一个被大多数机器复制的日志index
}
type AppendEntriesReply struct {
	Term int //leader的任期可能已经过时，此时收到Term用于更新leader自己
	Success bool //如果follower与Args中的PreLogIndex/PreLogTerm都匹配才会接过去新的日志（追加），不匹配直接返回false
	AppState AppendEntriesState  //追加的状态
	UpNextIndex int //用于更新请求节点的nextIndex
}


// --------------------------------------------------------领导选举----------------------------------------------------
//raft节点初始化 make
//Ticker 建立主体心跳 ticker
//投票rpc

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
//服务或测试人员想要创建一个Raft服务器。
//所有Raft服务器(包括这个)的端口都在peers[]中。该服务器的端口是peers[me]。所有服务器的peers[]数组顺序相同。
//Persister是服务器保存其持久状态的地方，如果有的话，它还最初保存了最近保存的状态。
//applyCh是一个通道，测试人员或服务希望Raft在该通道上发送ApplyMsg消息。
//Make()必须快速返回，因此它应该启动goroutines,用于任何长时间运行的工作。
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	rf.applyChan = applyCh //2B
	
	//服务器持久化的
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]LogEntry, 0)//创建一个len为0的logEntry类型的切片
	
	//服务器经常修改的
	rf.commitIndex = 0
	rf.lastApplied = 0

	//leader管理的
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	//追加的
	rf.status = Follower
	rf.overtime = time.Duration(150 + rand.Intn(200)) * time.Millisecond //随机产生150~350ms
	rf.timer = time.NewTicker(rf.overtime) //创建一个定时器

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		// 当定时器结束进行根据status，进行一次ticker
		select {
		case <-rf.timer.C:
			if rf.killed() {
				return
			}
			rf.mu.Lock()
			//根据自身status进行一次ticker
			switch rf.status {
			case Follower://follwer:变成竞选者
				rf.status = Candidate
				fallthrough
			case Candidate://candidate:发起投票

				//初始化自身任期，并把票投给自己
				rf.currentTerm += 1
				rf.votedFor = rf.me
				votedNums := 1

				//每轮选举开始时，重新设置选举超时
				rf.overtime = time.Duration(150 + rand.Intn(200)) * time.Millisecond
				rf.timer.Reset(rf.overtime)

				//对自身以外的节点发起选举
				for i:=0; i<len(rf.peers); i++{
					if i == rf.me{//跳过自己
						continue;
					}
					//创建投票的rpc
					voteArgs := RequestVoteArgs{
						Term: rf.currentTerm,
						CandidateId: rf.me,
						LastLogIndex: len(rf.logs),
						LastLogTerm: 0,//为什么不直接写：rf.logs[len(rf.logs)-1].Term,怕len为0导致数组访问越界
					}
					if(len(rf.logs) > 0){
						voteArgs.LastLogTerm = rf.logs[len(rf.logs)-1].Term
					}

					voteReply := RequestVoteReply{}

					go rf.sendRequestVote(i, &voteArgs, &voteReply,&votedNums)
				}
			case Leader://leader:发送心跳 / 日志同步
				appendNums := 1 //正确返回的节点数量
				rf.timer.Reset(HeartBeatTimeout)
				//对除leader自身外的每个节点发送心跳
				for i:=0; i<len(rf.peers); i++{
					if i == rf.me{
						continue
					}
					args := AppendEntriesArgs{
						Term: rf.currentTerm,
						LeaderId: rf.me,
						PrevLogIndex: 0,
						PrevLogTerm: 0,
						Entries: nil,
						LeaderCommit: rf.commitIndex,
					}
					reply := AppendEntriesReply{}

					//2b
					//如果追加到peer上对应下标nextIndex[i]！=rf.logs，则代表peer与leader的log entries不一致，需要将不一致的都附带过去
					args.Entries = rf.logs[rf.nextIndex[i]-1:]

					//代表不是初始0
					if rf.nextIndex[i]>0{
						args.PrevLogIndex = rf.nextIndex[i]-1
					}
					if args.PrevLogIndex > 0{
						args.PrevLogTerm = rf.logs[args.PrevLogIndex-1].Term
					}

					go rf.sendAppendEntries(i, &args, &reply,&appendNums)
				}
			}
			rf.mu.Unlock()	
		}
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
//首先竞选者的任期必须，大于自己的任期。否则返回false。因为在出先网络分区时，可能两个分区分别产生了两个leader。那么我们认为应该是任期长的leader拥有的数据更完整。
//投票成功的前提的是，你自己的票要没投过给别人，且竞选者的日志状态要和你的一致。

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, votedNums *int) bool {
	if rf.killed() {
		return false
	}

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok{ //失败重传
		if rf.killed(){
			return false
		}
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//sendRequestVote是并行的，因此存在一种情况就是有个reply告诉这个raft节点你落后了，(见reply的case)
	//然后更新了currentTerm，从而导致当前节点的currentTerm大于args.Term，所以不必再进行后面的操作了，
	//因为此时当前raft节点已经由Candidate变回follower了。
	if args.Term < rf.currentTerm{
		return false
	}

	//对reply情况进行分支处理
	switch reply.VoteState{
	case Expire:
		// fmt.Println("[sendRequestVote-func]:Expire")
		//消息过期有两种情况：
		//1.rf候选人的本身的term过期了，比节点小
		//2.rf候选人节点日志条目落后于节点了
		{
			rf.status = Follower
			rf.timer.Reset(rf.overtime)
			if reply.Term > rf.currentTerm{
				rf.currentTerm = reply.Term
				rf.votedFor = -1
			}
		}
	case Normal, Voted:
		// fmt.Println("[sendRequestVote-func]:*Normal,Voted")
		//收集选票数量
		if reply.VoteGranted && rf.currentTerm == reply.Term && *votedNums <= (len(rf.peers)/2){
			*votedNums++
			// fmt.Println("[sendRequestVote-func]:*votedNums++")
		}
		//票数超过一半
		if *votedNums >= (len(rf.peers)/2)+1 {
			//本来就是leader
			if rf.status == Leader{ 
				return ok
			}
			//本来不是leader,则让它成为leader，初始化next数组
			rf.status = Leader
			rf.nextIndex = make([]int, len(rf.peers))//每个peer对应一个nextIndex值
			for i, _ := range rf.nextIndex {
				rf.nextIndex[i] = len(rf.logs)+1//对于每一个服务器，需要发送给他的下一个日志条目的索引值
			}
			rf.timer.Reset(HeartBeatTimeout)
			// fmt.Println("[sendRequestVote-func]:the peer:", rf.me, "become leader")
		}
	case Killed:
		fmt.Println("[sendRequestVote-func]:killed")
		return false
	}
	return ok
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//当前节点crash
	if rf.killed(){
		reply.VoteState = Killed
		reply.Term = -1
		reply.VoteGranted = false
		return
	}

	//第一个要求：竞选者的Term必须大于自己的，否则可能出现网络分区：该竞选者已经out of date 过时了
	if args.Term < rf.currentTerm{
		reply.VoteState = Expire
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	//把currentTerm小于候选者的peer，都重置自身状态
	if args.Term > rf.currentTerm{
		rf.status = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	//此时比自己任期小peer的都已经把票还原
	if rf.votedFor == -1 {
		// fmt.Println("[RequestVote-func]:rf.votedFor == -1" )
		currentLogIndex := len(rf.logs)-1
		currentLogTerm := 0
		//如果currentLogIndex不为-1 ，则把term值复制过来
		if currentLogIndex >=0 {
			currentLogTerm = rf.logs[currentLogIndex].Term
		
		}
		//论文中第二个要求：候选人的日志至少和自己一样新。优先选任期大的，
		//如果任期相同，则说明同时有两个竞争者，两个一样的网络分区
		//那么这个时候再看日志条目数，此时args的日志下标就不能低于当前节点的日志下标。
		if args.LastLogTerm < currentLogTerm || (len(rf.logs) > 0 && args.LastLogTerm == rf.logs[len(rf.logs)-1].Term && args.LastLogIndex < len(rf.logs)){
			reply.VoteState = Expire
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
			return
		}
		//给票数，返回true
		rf.votedFor = args.CandidateId
		reply.VoteState = Normal
		reply.VoteGranted = true
		reply.Term = rf.currentTerm

		rf.timer.Reset(rf.overtime)
	} else{ //任期相同但是票已经给了,分两种情况
		reply.VoteState = Voted
		reply.VoteGranted = false

		//1.当前节点来自于同一轮，但是不同竞选者，但票已经给了别人 （有可能自己就是竞选者）
		if rf.votedFor != args.CandidateId {
			return
		}else { //2.当前节点的票已经给了同一个人，但由于sleep等网络原因，又发送了一次请求
			rf.status = Follower //重置自身状态
		}
		rf.timer.Reset(rf.overtime)
	}
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, appendNums *int) {
	if rf.killed(){
		return 
	}
	//论文5.3，如果append失败应该一直不断地entries，直到这个log成功被store
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	for !ok {
		if rf.killed() {
			return
		}
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
	}

	//必须在这里上锁，如果加在前面，前面retry时进入时，RPC也需要一个锁，但是又获取不到，因为锁已经被加上了
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	//对reply的返回状态进行分支
	switch reply.AppState{
	//目标节点crash
	case AppKilled:
		{
			return
		}
	//目标节点正常返回
	case AppNormal:
		{
			// 2A的test目的是让Leader能不能连续任期，所以2A只需要对节点初始化然后返回就好
			//2b:返回日志复制结果：需要判断返回的节点是否超过半数节点，才能将自身的日志commit，并apply到客户端
			//追加成功的，累加
			if reply.Success && reply.Term == rf.currentTerm && *appendNums <= (len(rf.peers)/2){
				*appendNums++
			}
			//节点的nextIndex大于日志数组长度
			if rf.nextIndex[server] > len(rf.logs)+1 {
				return
			}
			rf.nextIndex[server] += len(args.Entries)//更新对应该节点的nextIndex

			//追加成功数已经超过半数节点，则提交到客户端
			if *appendNums > len(rf.peers)/2{
				//保证幂等性：不会提交第二次
				*appendNums = 0

				if len(rf.logs)==0 || rf.logs[len(rf.logs)-1].Term != rf.currentTerm {
					return 
				}
				//发送applyMsg给客户端
				for rf.lastApplied <len(rf.logs){
					rf.lastApplied++
					applyMsg := ApplyMsg{
						CommandValid : true,
						CommandIndex : rf.lastApplied,
						Command : rf.logs[rf.lastApplied-1].Command,
					}
					rf.applyChan <- applyMsg
					rf.commitIndex = rf.lastApplied
					// fmt.Println("[sendAppendEntries-func]:",rf.me)
				}
			}
			return
		}
	case Mismatch:
		if args.Term != rf.currentTerm{
			return
		}
		//更新fllower实际需要的nextIndex
		rf.nextIndex[server]=reply.UpNextIndex
	
	
	//If AppendEntries RPC received from new leader: convert to follower(paper - 5.2)
	//出现网络分区，该Leader已经OutOfDate(过时）
	case AppOutOfDate:
		rf.status = Follower
		rf.votedFor = -1
		rf.currentTerm = reply.Term
		rf.timer.Reset(rf.overtime)

	case AppCommited:
		if args.Term != rf.currentTerm{
			return
		}
		rf.nextIndex[server] = reply.UpNextIndex
	}
	return
}


// AppendEntries 建立心跳，同步日志rpc
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//节点crash
	if rf.killed(){
		reply.AppState = AppKilled
		reply.Term = -1
		reply.Success = false
		return
	}

	//出现网络分区，args的任期比当前raft节点要小，说明之前args所在分区已经outofdate
	if args.Term < rf.currentTerm{
		reply.AppState = AppOutOfDate
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	//2b
	//追加日志冲突
	//（1）当前节点落后
	//1. 如果preLogIndex的大于当前节点日志的最大的下标说明follower缺失日志，拒绝附加日志
	//2. 如果当前节点preLogIndex处的任期Term和args.preLogTerm不相等，那么说明日志存在conflict,拒绝附加日志
	if args.PrevLogIndex >0 && (args.PrevLogIndex > len(rf.logs) || rf.logs[args.PrevLogIndex-1].Term != args.PrevLogTerm){
		reply.AppState = Mismatch
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.UpNextIndex = rf.lastApplied+1
		return;
	}
	//(2)当前节点超前
	//如果当前节点提交的index比传过来的高，说明当前的日志节点已经超前，需要返回过去
	if args.PrevLogIndex != -1 && rf.lastApplied > args.PrevLogIndex{
		reply.AppState = AppCommited
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.UpNextIndex = rf.lastApplied+1
		return
	}

	//对当前的rf进行ticker重置
	rf.currentTerm = args.Term
	rf.votedFor = args.LeaderId
	rf.status = Follower
	rf.timer.Reset(rf.overtime)

	//对返回的reply进行赋值
	reply.AppState = AppNormal
	reply.Term = rf.currentTerm
	reply.Success = true

	//2b
	//如果存在日志包，则进行追加
	if args.Entries != nil{
		rf.logs = rf.logs[:args.PrevLogIndex]
		rf.logs = append(rf.logs, args.Entries...)//将 args.Entries 切片中的所有元素追加到 rf.logs 切片的末尾。
	}
	//将日志提交与leader相同
	for rf.lastApplied < args.LeaderCommit{
		rf.lastApplied++
		applyMsg := ApplyMsg{
			CommandValid: true,
			CommandIndex: rf.lastApplied,
			Command: rf.logs[rf.lastApplied-1].Command,
		}
		rf.applyChan <- applyMsg //将日志存放到applyChan中
		rf.commitIndex = rf.lastApplied
	}
	return
}
// return currentTerm and whether this server
// believes it is the leader.
//判断当前的任期,以及是否时领导人
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	// fmt.Println("the peer :", rf.me, "term is:", rf.currentTerm ,"isleader is: ", isleader)
	if rf.status == Leader {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Exaple:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}






//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//第一个返回值是命令在提交后出现的索引。
//第二个返回值是当前任期。
//第三个返回值为：如果此服务器认为它是领导者，则第三个返回值为true。
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	if rf.killed() {
		return index, term, false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//如果不是leader，返回false
	if rf.status != Leader {
		return index, term, false
	}
	isLeader = true

	//初始化日志条目，并进行追加
	appendLog := LogEntry{Term: rf.currentTerm, Command: command}
	rf.logs = append(rf.logs, appendLog)
	index = len(rf.logs)
	term = rf.currentTerm

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	//stop计时
	rf.mu.Lock()
	rf.timer.Stop()
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}



