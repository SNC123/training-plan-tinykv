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
	"reflect"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// ErrStepLocalMsg is returned when try to step a local raft message
var ErrStepLocalMsg = errors.New("raft: cannot step raft local message")

// ErrStepPeerNotFound is returned when try to step a response message
// but there is no peer found in raft.Prs for that node.
var ErrStepPeerNotFound = errors.New("raft: cannot step as peer not found")

// SoftState provides state that is volatile and does not need to be persisted to the WAL.
type SoftState struct {
	Lead      uint64
	RaftState StateType
}

// Ready encapsulates the entries and messages that are ready to read,
// be saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.
// Ready表示：自上次Advance以来，Rawnode结点的改变量
type Ready struct {
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	*SoftState

	// The current state of a Node to be saved to stable storage BEFORE
	// Messages are sent.
	// HardState will be equal to empty state if there is no update.
	pb.HardState

	// Entries specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
	Entries []pb.Entry

	// Snapshot specifies the snapshot to be saved to stable storage.
	Snapshot pb.Snapshot

	// CommittedEntries specifies entries to be committed to a
	// store/state-machine. These have previously been committed to stable
	// store.
	CommittedEntries []pb.Entry

	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	// If it contains a MessageType_MsgSnapshot message, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.
	Messages []pb.Message
}

// RawNode is a wrapper of Raft.
type RawNode struct {
	Raft *Raft
	// 记录上次Advance后的状态
	prevSoftState *SoftState
	prevHardState pb.HardState
}

// NewRawNode returns a new RawNode given configuration and a list of raft peers.
func NewRawNode(config *Config) (*RawNode, error) {
	r := newRaft(config)
	return &RawNode{
		Raft:          r,
		prevSoftState: &SoftState{Lead: r.Lead, RaftState: r.State},
		prevHardState: pb.HardState{Term: r.Term, Vote: r.Vote, Commit: r.RaftLog.committed},
	}, nil
}

// Tick advances the internal logical clock by a single tick.
func (rn *RawNode) Tick() {
	rn.Raft.tick()
}

// Campaign causes this RawNode to transition to candidate state.
func (rn *RawNode) Campaign() error {
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgHup,
	})
}

// Propose proposes data be appended to the raft log.
func (rn *RawNode) Propose(data []byte) error {
	ent := pb.Entry{Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		From:    rn.Raft.id,
		Entries: []*pb.Entry{&ent}})
}

// ProposeConfChange proposes a config change.
func (rn *RawNode) ProposeConfChange(cc pb.ConfChange) error {
	data, err := cc.Marshal()
	if err != nil {
		return err
	}
	ent := pb.Entry{EntryType: pb.EntryType_EntryConfChange, Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{&ent},
	})
}

// ApplyConfChange applies a config change to the local node.
func (rn *RawNode) ApplyConfChange(cc pb.ConfChange) *pb.ConfState {
	if cc.NodeId == None {
		return &pb.ConfState{Nodes: nodes(rn.Raft)}
	}
	switch cc.ChangeType {
	case pb.ConfChangeType_AddNode:
		rn.Raft.addNode(cc.NodeId)
	case pb.ConfChangeType_RemoveNode:
		rn.Raft.removeNode(cc.NodeId)
	default:
		panic("unexpected conf type")
	}
	return &pb.ConfState{Nodes: nodes(rn.Raft)}
}

// Step advances the state machine using the given message.
func (rn *RawNode) Step(m pb.Message) error {
	// ignore unexpected local messages receiving over network
	if IsLocalMsg(m.MsgType) {
		return ErrStepLocalMsg
	}
	if pr := rn.Raft.Prs[m.From]; pr != nil || !IsResponseMsg(m.MsgType) {
		return rn.Raft.Step(m)
	}
	return ErrStepPeerNotFound
}

// Ready returns the current point-in-time state of this RawNode.
func (rn *RawNode) Ready() Ready {
	ss := &SoftState{
		Lead:      rn.Raft.Lead,
		RaftState: rn.Raft.State,
	}
	hs := pb.HardState{
		Term:   rn.Raft.Term,
		Vote:   rn.Raft.Vote,
		Commit: rn.Raft.RaftLog.committed,
	}

	rd := Ready{
		Entries:          rn.Raft.RaftLog.unstableEntries(),
		CommittedEntries: rn.Raft.RaftLog.nextEnts(),
		Messages:         rn.Raft.msgs,
	}

	if rn.Raft.State != StateLeader && rn.Raft.RaftLog.pendingSnapshot != nil {
		rd.Snapshot = *rn.Raft.RaftLog.pendingSnapshot
	}

	// SoftState 仅在变化时返回
	if !reflect.DeepEqual(rn.prevSoftState, ss) {
		rd.SoftState = ss
	}

	// HardState 若无变化，则返回空值
	if !reflect.DeepEqual(rn.prevHardState, hs) {
		rd.HardState = hs
	}

	return rd
}

// HasReady called when RawNode user need to check if any Ready pending.
// 自上次Advance以来，状态是否有变化
func (rn *RawNode) HasReady() bool {
	ss := &SoftState{Lead: rn.Raft.Lead, RaftState: rn.Raft.State}
	if !reflect.DeepEqual(rn.prevSoftState, ss) {
		return true
	}
	hs := pb.HardState{Term: rn.Raft.Term, Vote: rn.Raft.Vote, Commit: rn.Raft.RaftLog.committed}
	// empty_hs := pb.HardState{}
	if !reflect.DeepEqual(rn.prevHardState, hs) {
		return true
	}
	if len(rn.Raft.RaftLog.unstableEntries()) > 0 {
		return true
	}
	if len(rn.Raft.RaftLog.nextEnts()) > 0 {
		return true
	}
	if len(rn.Raft.msgs) > 0 {
		return true
	}
	if rn.Raft.State != StateLeader && rn.Raft.RaftLog.pendingSnapshot != nil {
		return true
	}
	return false
}

// Advance notifies the RawNode that the application has applied and saved progress in the
// last Ready results.
// Ready的改变量内容已由外界完成（如apply和persist），更新状态
func (rn *RawNode) Advance(rd Ready) {
	if len(rd.Entries) > 0 {
		last := rd.Entries[len(rd.Entries)-1]
		rn.Raft.RaftLog.stabled = last.Index
	}
	if len(rd.CommittedEntries) > 0 {
		last := rd.CommittedEntries[len(rd.CommittedEntries)-1]
		rn.Raft.RaftLog.applied = last.Index

		// var state string
		// if rn.Raft.State == StateLeader {
		// 	state = "leader"
		// } else {
		// 	state = "follower"
		// }
		// log.DIYf("advance", "raft %v (%v) applied entries [%v-%v] committed = %v, applied = %v",
		// 	rn.Raft.id, state,
		// 	rd.CommittedEntries[0].Index, rd.CommittedEntries[len(rd.CommittedEntries)-1].Index,
		// 	rn.Raft.RaftLog.committed, rn.Raft.RaftLog.applied,
		// )
	}
	rn.Raft.msgs = nil
	rn.prevSoftState = &SoftState{Lead: rn.Raft.Lead, RaftState: rn.Raft.State}
	rn.prevHardState = pb.HardState{Term: rn.Raft.Term, Vote: rn.Raft.Vote, Commit: rn.Raft.RaftLog.committed}
	if !IsEmptySnap(&rd.Snapshot) {
		rn.Raft.RaftLog.pendingSnapshot = nil
	}

}

// GetProgress return the Progress of this node and its peers, if this
// node is leader.
func (rn *RawNode) GetProgress() map[uint64]Progress {
	prs := make(map[uint64]Progress)
	if rn.Raft.State == StateLeader {
		for id, p := range rn.Raft.Prs {
			prs[id] = *p
		}
	}
	return prs
}
