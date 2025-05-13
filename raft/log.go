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
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	// 即 持久化的非压缩日志存储于此
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	// 未压缩日志的缓存，Client的Proposal首先加入此处
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}

	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}

	ents, err := storage.Entries(firstIndex, lastIndex+1)
	if err != nil {
		panic(err)
	}

	dummyIndex := firstIndex - 1
	dummyTerm, err := storage.Term(dummyIndex)
	if err != nil {
		panic(err)
	}
	hardState, _, _ := storage.InitialState()
	return &RaftLog{
		storage:   storage,
		entries:   append([]pb.Entry{{Index: dummyIndex, Term: dummyTerm}}, ents...), // 用于统一prevTerm边界问题
		committed: hardState.Commit,
		applied:   firstIndex - 1,
		stabled:   lastIndex,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() (uint64, uint64, uint64) {
	// Your Code Here (2C).

	persistFirstIndex, err := l.storage.FirstIndex()
	if err != nil || len(l.entries) <= 1 {
		return 0, 0, 0
	}
	dummyIndex := l.entries[0].Index
	memoryFirstIndex := dummyIndex + 1
	if memoryFirstIndex < persistFirstIndex {
		compactOffset := persistFirstIndex - dummyIndex
		l.entries = l.entries[compactOffset-1:]
		return persistFirstIndex - memoryFirstIndex, memoryFirstIndex, persistFirstIndex - 1
	}
	return 0, 0, 0
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	if len(l.entries) <= 1 {
		return nil
	}
	return l.entries[1:]
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	if len(l.entries) <= 1 {
		return []pb.Entry{}
	}

	dummyIndex := l.entries[0].Index // 对应storage.entries[1]的log index
	unstabled_offset := l.stabled + 1 - dummyIndex
	if unstabled_offset < 1 || int(unstabled_offset) >= len(l.entries) {
		return []pb.Entry{}
	}
	return l.entries[unstabled_offset:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	if len(l.entries) <= 1 {
		return nil
	}
	dummyIndex := l.entries[0].Index
	unapplied_offset := l.applied + 1 - dummyIndex
	uncommitted_offset := l.committed + 1 - dummyIndex
	if unapplied_offset < 1 || int(uncommitted_offset) > len(l.entries) || unapplied_offset >= uncommitted_offset {
		return nil
	}
	return l.entries[unapplied_offset:uncommitted_offset]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {

	// Raftlog的日志缓存（entries）有数据
	if len(l.entries) > 0 {
		return l.entries[len(l.entries)-1].Index
	}
	// Raftlog日志缓存无数据
	lastIndex, _ := l.storage.LastIndex()
	return lastIndex
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {

	dummyIndex := l.entries[0].Index
	if i < dummyIndex {
		return 0, ErrCompacted
	}
	offset := i - dummyIndex
	if offset >= uint64(len(l.entries)) {
		return 0, ErrUnavailable
	}
	return l.entries[offset].Term, nil
}

func (l *RaftLog) EntriesFrom(from uint64) ([]pb.Entry, error) {
	if len(l.entries) == 0 {
		return nil, nil
	}

	dummyIndex := l.entries[0].Index
	lastIndex := l.LastIndex()

	if from > lastIndex {
		return nil, nil // 没有日志可返回
	}

	if from <= dummyIndex {
		return nil, ErrCompacted // 被 snapshot 截断了
	}

	offset := int(from - dummyIndex)
	return l.entries[offset:], nil
}

func (l *RaftLog) GetCommitted() uint64 {
	return l.committed
}

func (l *RaftLog) GetApplied() uint64 {
	return l.applied
}
