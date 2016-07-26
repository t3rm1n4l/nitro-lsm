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

type ValidNodeFn func(*Node) bool

func defaultValidNode(*Node) bool {
	return true
}

func (s *Skiplist) ExecBatchOps(opItr BatchOpIterator, head, tail *Node,
	callb BatchOpCallback, cmp CompareFn,
	validNode ValidNodeFn, sts *Stats) error {

	// Maxlevel
	var level int

	if validNode == nil {
		validNode = defaultValidNode
	}

	if head == nil {
		head = s.head
		level = int(s.level)
	} else {
		level = head.Level()
	}

	if tail == nil {
		tail = s.tail
	}

	err := s.execBatchOpsInner(head, tail, level, opItr,
		cmp, validNode, callb, sts)

	if err != nil {
		return err
	}

	if opItr.Valid() {
		panic("non-zero items remaining")
	}

	return err
}

func (s *Skiplist) execBatchOpsInner(startNode, endNode *Node, level int,
	opItr BatchOpIterator, cmp CompareFn, validNode ValidNodeFn,
	callb BatchOpCallback, sts *Stats) (err error) {

	currNode := startNode

	// Iterate in the current level
	for Compare(cmp, currNode.Item(), endNode.Item()) < 0 && opItr.Valid() {
		var rightNode *Node
		for rightNode, _ = currNode.getNext(level); !validNode(rightNode); {
			rightNode, _ = rightNode.getNext(level)
		}

		// Descend to the next level
		if Compare(cmp, opItr.Item(), rightNode.Item()) < 0 {
			if level == 0 {
				if err = callb(currNode, cmp, rightNode.Item(), opItr); err != nil {
					return
				}
			} else {
				if err = s.execBatchOpsInner(currNode, rightNode, level-1, opItr,
					cmp, validNode, callb, sts); err != nil {
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
