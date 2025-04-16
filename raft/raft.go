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
	// Your Code Here (2A).
	prs := make(map[uint64]*Progress)
	votes := make(map[uint64]bool)

	for _, id := range c.peers {
		prs[id] = &Progress{
			Match: 0,
			Next:  1,
		}
		votes[id] = false
	}
	new_raft := &Raft{
		id:               c.ID,
		Term:             0,
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

// []*pb.Entry转[]pb.Entry函数
func toEntrys(ptrs []*pb.Entry) []pb.Entry {
	res := make([]pb.Entry, len(ptrs))
	for i, p := range ptrs {
		res[i] = pb.Entry{
			Term:      p.Term,
			Index:     p.Index,
			Data:      p.Data,
			EntryType: p.EntryType,
		}
	}
	return res
}

// 给定一个AppendResp的match index，检测该index是否已经超过半数,过半则更新
// 返回值：是否有更新
func (r *Raft) maybeUpdateCommit(index uint64) (is_updated bool) {
	if r.State != StateLeader {
		panic("only leader need to check commit")
	}
	committed_count := 1
	for _, knownPeerIndex := range r.Prs {
		if index <= knownPeerIndex.Match {
			committed_count++
		}
		if committed_count > len(r.Prs)/2 {
			r.RaftLog.committed = max(r.RaftLog.committed, index)
			return true
		}
	}
	return false
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
	// Your Code Here (2A).
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
	// 添加空操作日志
	if len(ents) == 0 {
		ents = []pb.Entry{{
			Term:  r.Term,
			Index: nextIndex,
			Data:  nil,
		}}
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

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.sendMsg(pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
	})
}

func (r *Raft) sendRequestVote(to uint64) {
	lastIndex := r.RaftLog.LastIndex()
	lastTerm, err := r.RaftLog.Term(lastIndex)
	if err != nil {
		panic("failed to send request vote")
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
	// Your Code Here (2A).	switch r.State {
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
	// Your Code Here (2A).
	r.State = StateFollower
	r.Lead = lead
	r.Term = term
	r.Vote = 0 // 注意清空Vote，确保每个term仅投票一次
	r.resetRandomizedElectionTimeout()
}

// becomeCandidate transform this peer's state to candidate
// 发觉leader故障，进行新一轮选举
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term++                        // 仅在发起新选举时增加term
	r.votes = make(map[uint64]bool) // 注意清空votes
	r.resetRandomizedElectionTimeout()
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
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
	// TODO 优化速度?
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

/* ==================  become函数部分 END ================== */

/* ==================  step函数部分 START ================== */

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
// 即 当前raft结点收到新信息Message后该如何处理
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	// 收到更大term的消息，应该强制更新
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
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		// 广播Message RequestVote
		for id := range r.Prs {
			if id == r.id {
				r.votes[r.id] = true
				continue
			}
			r.sendRequestVote(id)
		}
		// 避免特殊情况：集群中只有一个节点
		r.maybeBecomeLeader()
	case pb.MessageType_MsgRequestVote:
		lastIndex := r.RaftLog.LastIndex()
		lastTerm, err := r.RaftLog.Term(lastIndex)
		if err != nil {
			panic("failed to get last entry's term")
		}
		if m.LogTerm > lastTerm || (m.LogTerm == lastTerm && m.Index >= lastIndex) {
			if r.Vote == 0 || r.Vote == m.From {
				r.sendRequestVoteResp(m.From, false)
				r.Vote = m.From
			} else {
				r.sendRequestVoteResp(m.From, true)
			}
		} else {
			r.sendRequestVoteResp(m.From, true)
		}
	case pb.MessageType_MsgAppend:
		r.Term = max(r.Term, m.Term)
		if m.Commit > r.RaftLog.committed {
			r.RaftLog.committed = m.Commit
		}

		// 一致性检查,确保(prevIndex,prevTerm)存在
		is_matched, matchIndex := r.findMatchedLogIndex(m.Index, m.LogTerm)
		if !is_matched {
			r.sendAppendResp(m.From, true, 0)
		} else {
			r.sendAppendResp(m.From, false, matchIndex)
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
		}
		// TODO 考虑是否需要维护lead信息

	}
	return nil
}
func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		// 广播Message RequestVote
		for id := range r.Prs {
			if id == r.id {
				r.votes[r.id] = true
				continue
			}
			r.sendRequestVote(id)
		}
		// 避免特殊情况：集群中只有一个节点
		r.maybeBecomeLeader()
	case pb.MessageType_MsgRequestVote:
		if m.Term > r.Term {
			r.sendRequestVoteResp(m.From, false)
			r.becomeFollower(m.Term, m.From)
		} else {
			r.sendRequestVoteResp(m.From, true)
		}
	case pb.MessageType_MsgRequestVoteResponse:
		if !m.Reject {
			r.votes[m.From] = true
		}
		r.maybeBecomeLeader()
	case pb.MessageType_MsgAppend:
		if r.Term <= m.Term {
			r.becomeFollower(m.Term, m.From)
		}
	}
	return nil
}
func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		// 广播Message HeartBeat
		for id := range r.Prs {
			if id == r.id {
				continue
			}
			r.sendHeartbeat(id)
		}

	case pb.MessageType_MsgAppend:
		if r.Term < m.Term {
			r.becomeFollower(m.Term, m.From)
		} else if r.Term == m.Term {
			panic("[Fatal Logic Error] multiple leaders in a term !!!")
		}
	case pb.MessageType_MsgAppendResponse:
		id := m.From
		if !m.Reject {
			r.Prs[id].Match = m.Index
			r.Prs[id].Next = m.Index + 1
			// 某个Index半数以上回应，更新Committed
			matchTerm, err := r.RaftLog.Term(m.Index)
			if err != nil {
				panic("[Leader@AppendResponse] Failed to get match entry's term ")
			}
			if matchTerm == r.Term {
				r.maybeUpdateCommit(m.Index)
			}
		} else {
			// TODO 解决拒绝情况的Progress维护，此时RESP中Index的含义是？
			r.Prs[id].Next = max(1, m.Index)
			r.sendAppend(id)
		}

	case pb.MessageType_MsgPropose:
		startIndex := r.RaftLog.LastIndex() + 1
		// 将message的日志追加入Raftlog
		for offset, ent := range m.Entries {
			r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
				Term:  r.Term,
				Index: startIndex + uint64(offset),
				Data:  ent.Data,
			})
		}
		// 广播新日志
		for id := range r.Prs {
			if id == r.id {
				continue
			}
			if is_send := r.sendAppend(id); !is_send {
				panic("failed when sending append message")
			}
		}
		// 避免特殊情况：集群中只有一个节点
		r.maybeUpdateCommit(r.RaftLog.LastIndex())
	}

	return nil
}

/* ==================  step函数部分 END ================== */

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
}

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
