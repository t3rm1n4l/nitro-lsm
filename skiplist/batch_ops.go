package skiplist

import (
	"unsafe"
)

type BatchOpIterator interface {
	Next()
	Valid() bool
	Item() unsafe.Pointer
}

type AcceptFn func(unsafe.Pointer) bool

type BatchOpCallback func(*Node, CompareFn, unsafe.Pointer, BatchOpIterator) error

func (s *Skiplist) ExecBatchOps(opItr BatchOpIterator, callb BatchOpCallback,
	cmp CompareFn, sts *Stats) error {
	err := s.execBatchOpsInner(s.head, s.tail, int(s.level), opItr,
		cmp, callb, sts)

	if err != nil {
		return err
	}

	if opItr.Valid() {
		panic("non-zero items remaining")
	}

	return err
}

func (s *Skiplist) execBatchOpsInner(startNode, endNode *Node, level int,
	opItr BatchOpIterator, cmp CompareFn,
	callb BatchOpCallback, sts *Stats) (err error) {

	currNode := startNode

	// Iterate in the current level
	for Compare(cmp, currNode.Item(), endNode.Item()) < 0 && opItr.Valid() {
		rightNode, _ := currNode.getNext(level)

		// Descend to the next level
		if Compare(cmp, opItr.Item(), rightNode.Item()) < 0 {
			if level == 0 {
				if err = callb(currNode, cmp, rightNode.Item(), opItr); err != nil {
					return
				}
			} else {
				if err = s.execBatchOpsInner(currNode, rightNode, level-1, opItr,
					cmp, callb, sts); err != nil {
					return
				}
			}
		}

		currNode = rightNode
		if currNode == nil {
			break
		}
	}

	return
}
