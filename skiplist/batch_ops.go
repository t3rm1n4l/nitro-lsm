package skiplist

import (
	"fmt"
	"unsafe"
)

type BatchOp struct {
	Flag int
	Itm  unsafe.Pointer
}

type BatchOpCallback func(*Node, []BatchOp) error

func (s *Skiplist) ExecBatchOps(ops []BatchOp, callb BatchOpCallback,
	cmp CompareFn, sts *Stats) error {
	remaining, err := s.execBatchOpsInner(s.head, s.tail, int(s.level), ops,
		cmp, callb, sts)

	if err != nil {
		return err
	}

	if len(remaining) > 0 {
		panic(fmt.Sprintf("non-zero items remaining %d", len(remaining)))
	}

	return err
}

func (s *Skiplist) execBatchOpsInner(startNode, endNode *Node, level int,
	ops []BatchOp, cmp CompareFn,
	callb BatchOpCallback, sts *Stats) (currOps []BatchOp, err error) {

	currOps = ops
	currNode := startNode

	// Iterate in the current level
	for compare(cmp, currNode.Item(), endNode.Item()) < 0 && len(currOps) > 0 {
		rightNode, _ := currNode.getNext(level)

		// Descend to the next level
		if compare(cmp, currOps[0].Itm, rightNode.Item()) < 0 {
			if level == 0 {
				offset := 1
				for offset < len(currOps) &&
					compare(cmp, currOps[offset].Itm, rightNode.Item()) < 0 {
					offset++
				}

				if err = callb(currNode, currOps[0:offset]); err != nil {
					return
				}

				currOps = currOps[offset:] // Remaining
			} else {
				if currOps, err = s.execBatchOpsInner(currNode, rightNode, level-1, currOps,
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
