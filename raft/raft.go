// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"math/rand"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
	// progress是针对特定follower的，在leader的map结构Prs中存在
	// match ：在leader眼中，该follower已经匹配上的日志索引
	// next：在leader眼中，该follower下一个需要的日志索引
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	// 最小超时时间
	electionTimeout int
	// 当前轮时机超时值（每轮随机生成）
	randomizedElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	prs := make(map[uint64]*Progress)
	votes := make(map[uint64]bool)

	for _, id := range c.peers {
		lastIndex, err := c.Storage.LastIndex()
		if err != nil {
			panic("[newRaft] Failed to get persisted last entry's index ")
		}
		prs[id] = &Progress{
			Match: lastIndex,
			Next:  lastIndex + 1,
		}
		votes[id] = false
	}
	hardState, _, _ := c.Storage.InitialState()

	new_raft := &Raft{
		id:               c.ID,
		Term:             hardState.Term,
		Vote:             hardState.Vote,
		Prs:              prs,
		RaftLog:          newLog(c.Storage),
		votes:            votes,
		State:            StateFollower,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		heartbeatElapsed: 0,
		electionElapsed:  0,
		leadTransferee:   0,
	}
	// 设置随机超时时间
	new_raft.resetRandomizedElectionTimeout()
	return new_raft
}

/* ==================  工具函数部分 START ================== */

// 重新生成超时时间
func (r *Raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// []pb.Entry转[]*pb.Entry函数
func toEntryPtrs(ents []pb.Entry) []*pb.Entry {
	res := make([]*pb.Entry, len(ents))
	for i := range ents {
		res[i] = &ents[i]
	}
	return res
}

// 给定一个AppendResp的match index，检测该index是否已经超过半数,过半则更新
// 返回值：是否有更新
func (r *Raft) maybeUpdateCommit(index uint64) {
	if r.State != StateLeader {
		panic("[maybeUpdateCommit] Only leader need to check commit ")
	}
	committed_count := 0
	for _, knownPeerIndex := range r.Prs {
		if index <= knownPeerIndex.Match {
			committed_count++
		}
		if committed_count > len(r.Prs)/2 {
			if index > r.RaftLog.committed {
				r.RaftLog.committed = index
				// 按照hint4 要求，commit更新后立刻广播
				r.broadcast(func(id uint64) {
					r.sendAppend(id)
				})
				return
			}
		}
	}
}

// 一致性检查，找不到则返回(0,false)
func (r *Raft) findMatchedLogIndex(prevIndex uint64, prevTerm uint64) (bool, uint64) {
	for i := len(r.RaftLog.entries) - 1; i >= 0; i-- {
		ent := r.RaftLog.entries[i]
		if ent.Index == prevIndex && ent.Term == prevTerm {
			return true, ent.Index
		}
	}
	return false, 0
}

// 通用广播消息函数
func (r *Raft) broadcast(f func(id uint64)) {
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		f(id)
	}
}

/* ==================  工具函数部分 END ================== */

/* ==================  send函数部分 START ================== */

// 发送信息
// 即将message加入msg数组（实际是待发送队列)
func (r *Raft) sendMsg(m pb.Message) error {
	r.msgs = append(r.msgs, m)
	return nil
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	prs := r.Prs[to]
	nextIndex := prs.Next
	prevIndex := nextIndex - 1

	prevTerm, err := r.RaftLog.Term(prevIndex)
	if err != nil {
		return false
	}
	ents, err := r.RaftLog.EntriesFrom(nextIndex)
	if err != nil {
		return false
	}

	r.sendMsg(pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Index:   prevIndex,
		LogTerm: prevTerm,
		Entries: toEntryPtrs(ents),
		Commit:  r.RaftLog.committed,
	})
	return true
}

func (r *Raft) sendAppendResp(to uint64, reject bool, matchIndex uint64) {
	r.sendMsg(pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
		Index:   matchIndex,
	})
}

func (r *Raft) sendHeartbeat(to uint64) {
	r.sendMsg(pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
	})
}

func (r *Raft) sendHeartbeatResp(to uint64, reject bool) {
	r.sendMsg(pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
	})
}

func (r *Raft) sendRequestVote(to uint64) {
	lastIndex := r.RaftLog.LastIndex()
	lastTerm, err := r.RaftLog.Term(lastIndex)
	if err != nil {
		panic("[send@ReqeustVote] Failed to send request vote ")
	}
	r.sendMsg(pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Index:   lastIndex,
		LogTerm: lastTerm,
	})
}

func (r *Raft) sendRequestVoteResp(to uint64, reject bool) {
	r.sendMsg(pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
	})
}

func (r *Raft) sendPropose(to uint64, ents []*pb.Entry) {
	r.sendMsg(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		From:    r.id,
		To:      to,
		Entries: ents,
	})
}

/* ==================  send函数部分 END ================== */

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	switch r.State {
	case StateFollower, StateCandidate:
		r.electionElapsed++
		if r.electionElapsed >= r.randomizedElectionTimeout {
			r.electionElapsed = 0
			// 发起内部广播选举Message
			r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
		}
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			// 发起内部广播心跳Message
			r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
		}
	}
}

/* ==================  become函数部分 START ================== */

// becomeFollower transform this peer's state to Follower
// 即该raft结点知道leader存在，转变为follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.State = StateFollower
	r.Lead = lead
	r.Term = term
	r.Vote = 0 // 注意清空Vote，确保每个term仅投票一次
	r.resetRandomizedElectionTimeout()
}

// becomeCandidate transform this peer's state to candidate
// 发觉leader故障，进行新一轮选举
func (r *Raft) becomeCandidate() {
	r.State = StateCandidate
	r.Term++                        // 仅在发起新选举时增加term
	r.votes = make(map[uint64]bool) // 注意清空votes
	r.resetRandomizedElectionTimeout()
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	// 执行一个空Propose操作，用于对齐committed
	noop_ent := []*pb.Entry{{Data: nil}}
	r.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		From:    r.id,
		To:      r.id,
		Entries: noop_ent,
	})
}

// 检验票数是否过半，过半则变为Leader
func (r *Raft) maybeBecomeLeader() {
	var agree_count = 0
	for _, agree := range r.votes {
		if agree {
			agree_count++
		}
	}
	if agree_count > len(r.Prs)/2 {
		r.becomeLeader()
	}
}

// 检验是否收到到所有回复，仍未过半则变为Follower
func (r *Raft) maybeBecomeFollower() {
	var agree_count = 0
	for _, agree := range r.votes {
		if agree {
			agree_count++
		}
	}
	if len(r.votes) == len(r.Prs) && agree_count <= len(r.Prs)/2 {
		r.becomeFollower(r.Term, 0)
	}
}

/* ==================  become函数部分 END ================== */

/* ==================  step函数部分 START ================== */

// Raft 消息处理的入口
// 不同类型的消息请参见 `eraftpb.proto` 中的 `MessageType` 枚举
// 即：当前 Raft 节点在收到 Message 后的处理逻辑从此处开始
func (r *Raft) Step(m pb.Message) error {

	// 任何角色收到任何更大term的消息，应该强制变为follower
	if r.Term < m.Term {
		r.becomeFollower(m.Term, 0) // 此时并不知道谁是leader
	}

	switch r.State {
	case StateFollower:
		r.stepFollower(m)
	case StateCandidate:
		r.stepCandidate(m)
	case StateLeader:
		r.stepLeader(m)
	}
	return nil
}

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHeartbeat:
		if m.Term >= r.Term {
			r.sendHeartbeatResp(m.From, false)
		} else {
			r.sendHeartbeatResp(m.From, true)
		}
	case pb.MessageType_MsgHup:
		r.handleHub(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	}
	return nil
}
func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.handleHub(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		if !m.Reject {
			r.votes[m.From] = true
		} else {
			r.votes[m.From] = false
		}
		r.maybeBecomeLeader()
		r.maybeBecomeFollower()
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	}
	return nil
}
func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		// 广播Message HeartBeat
		r.broadcast(func(id uint64) {
			r.sendHeartbeat(id)
		})
	case pb.MessageType_MsgHeartbeatResponse:
		if !m.Reject {
			// 如果Follower缺少日志，应该补全
			if r.Prs[m.From].Match <= r.RaftLog.committed {
				r.sendAppend(m.From)
			}
		} else {
			// 这个部分不应该触发,在Step主入口处理
			panic("[stepLeader@HeartbeatResponse] HeartbeatResponse 'reject = true' should not be handled here ")
		}
	case pb.MessageType_MsgAppendResponse:
		id := m.From
		if !m.Reject {
			r.Prs[id].Match = m.Index
			r.Prs[id].Next = m.Index + 1
			// 某个Index半数以上回应，更新Committed
			matchTerm, err := r.RaftLog.Term(m.Index)
			if err != nil {
				panic("[stepLeader@AppendResponse] Failed to get match entry's term ")
			}
			// 只有当前term日志项才可作为commitIndex
			if matchTerm == m.Term {
				r.maybeUpdateCommit(m.Index)
			}
		} else {
			// AppendResponse为Reject,应回退一位
			// TODO 优化回退速度（例如按任期加速回退）
			r.Prs[id].Next = max(1, r.Prs[id].Next-1)
			r.sendAppend(id)
		}
	case pb.MessageType_MsgPropose:
		startIndex := r.RaftLog.LastIndex() + 1
		r.Prs[r.id].Match = r.RaftLog.LastIndex() + uint64(len(m.Entries))
		r.Prs[r.id].Next = r.Prs[r.id].Match + 1
		// 将message的日志追加入Raftlog
		for offset, ent := range m.Entries {
			r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
				Term:  r.Term,
				Index: startIndex + uint64(offset),
				Data:  ent.Data,
			})
		}
		// 广播Append新日志
		r.broadcast(func(id uint64) {
			r.sendAppend(id)
		})
		// 避免特殊情况：集群中只有一个节点
		r.maybeUpdateCommit(r.RaftLog.LastIndex())
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)

	}
	return nil
}

/* ==================  step函数部分 END ================== */

/* ==================  handle函数部分 START ==================*/

// 处理所有MsgRequestVote的逻辑，发起投票广播
func (r *Raft) handleHub(_ pb.Message) {
	switch r.State {
	case StateFollower, StateCandidate:
		r.becomeCandidate()
		r.votes[r.id] = true
		// 广播Message RequestVote
		r.broadcast(func(id uint64) {
			r.sendRequestVote(id)
		})
		// 避免特殊情况：集群中只有一个节点
		r.maybeBecomeLeader()
		r.maybeBecomeFollower()
	case StateLeader:
		panic("[stepLeader@Hub] Unexpected MsgHub to leader ")
	}

}

// 处理所有MsgRequestVote的逻辑，向其他Candidate投票，Follower只会向拥有更加新日志的Candidate投赞成票
func (r *Raft) handleRequestVote(m pb.Message) {
	switch r.State {
	case StateFollower:
		lastIndex := r.RaftLog.LastIndex()
		lastTerm, err := r.RaftLog.Term(lastIndex)
		if err != nil {
			panic("[stepFollower@RequestVote] Failed to get last entry's term ")
		}
		// 确保Candidate拥有更加新的日志
		if m.LogTerm > lastTerm || (m.LogTerm == lastTerm && m.Index >= lastIndex) {
			// 确保只投一次票
			if r.Vote == 0 || r.Vote == m.From {
				r.sendRequestVoteResp(m.From, false)
				r.Vote = m.From
			} else {
				r.sendRequestVoteResp(m.From, true)
			}
		} else {
			r.sendRequestVoteResp(m.From, true)
		}
	case StateCandidate, StateLeader:
		// Candidate和Leader收到同任期的票，应该拒绝
		// Message任期更大的情况已在Step()处理
		if m.Term == r.Term {
			r.sendRequestVoteResp(m.From, true)
		}
	}
}

// 处理所有MsgAppend的逻辑，实现Follower本地日志与Leader发送日志的同步，其中Follower为核心
func (r *Raft) handleAppendEntries(m pb.Message) {
	switch r.State {
	/*
		Follower在收到Leader发来的Message后，需要进行：
		1. 一致性检查
		2. 删除冲突位置（若有）之后的所有日志，并添加新日志
	*/
	case StateFollower:
		r.Term = max(r.Term, m.Term)
		r.Lead = m.From
		// 一致性检查,确保(prevIndex,prevTerm)存在
		is_matched, matchIndex := r.findMatchedLogIndex(m.Index, m.LogTerm)
		if !is_matched {
			r.sendAppendResp(m.From, true, matchIndex)
		} else {
			// 从(prevIndex,prevTerm)后第一条开始，匹配原日志和Message日志，若某个不一样则删除后续所有
			startIndex := matchIndex + 1
			// 找到matchIndex的offset
			startOffset := 0
			for i, ent := range r.RaftLog.entries {
				if ent.Index == matchIndex {
					startOffset = 1 + i
					break
				}
			}
			// 找到冲突位置的offset
			conflictOffset := -1
			for i, ent := range m.Entries {
				targetIndex := startIndex + uint64(i)
				if startOffset+i >= len(r.RaftLog.entries) {
					break
				}
				targetEntry := r.RaftLog.entries[startOffset+i]
				if targetEntry.Index != targetIndex || targetEntry.Term != ent.Term {
					conflictOffset = startOffset + i
					break
				}
			}
			// 发现冲突：删除冲突及后续，再追加新的
			// 未发现冲突：追加message中多出的日志
			if conflictOffset != -1 {
				r.RaftLog.stabled = r.RaftLog.entries[0].Index + uint64(conflictOffset) - 1
				r.RaftLog.entries = r.RaftLog.entries[:conflictOffset]
				for i := conflictOffset - startOffset; i < len(m.Entries); i++ {
					r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[i])
				}
			} else if startOffset+len(m.Entries) > len(r.RaftLog.entries) {
				for i := len(r.RaftLog.entries) - startOffset; i < len(m.Entries); i++ {
					r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[i])
				}
			}
			// Follower的commit更新
			if m.Commit > r.RaftLog.committed {
				if m.Entries != nil {
					r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())
				} else {
					// 特殊处理Message日志为空的情况，此时应以Message的prevIndex为准
					r.RaftLog.committed = min(m.Commit, m.Index)
				}
			}
			r.sendAppendResp(m.From, false, r.RaftLog.LastIndex())
		}
	case StateCandidate:
		if r.Term == m.Term {
			r.becomeFollower(m.Term, m.From)
		}
	case StateLeader:
		panic("[stepLeader@Append] Term check and role downgrade are already handled in Step()  ")
	}

}

/* ==================  handle函数部分 END ==================*/

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
