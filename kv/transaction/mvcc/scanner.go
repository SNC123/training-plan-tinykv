package mvcc

import (
	"bytes"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	iter engine_util.DBIterator
	txn  *MvccTxn
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	iter.Seek(EncodeKey(startKey, txn.StartTS))
	return &Scanner{
		txn:  txn,
		iter: iter,
	}
}

func (scan *Scanner) Close() {
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {

	if scan.iter.Valid() {
		item := scan.iter.Item()
		itemKey := item.Key()
		userKey := DecodeUserKey(itemKey)

		currentKey := userKey
		val, err := scan.txn.GetValue(currentKey)
		if err != nil {
			return nil, nil, err
		}
		// 移动到下一个user key
		for scan.iter.Next(); scan.iter.Valid(); scan.iter.Next() {
			nextUserKey := DecodeUserKey(scan.iter.Item().Key())
			if !bytes.Equal(nextUserKey, currentKey) {
				break
			}
		}

		return currentKey, val, nil
	}
	return nil, nil, nil
}
