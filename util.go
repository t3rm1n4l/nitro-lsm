// Copyright (c) 2016 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package nitro

import (
	"unsafe"
)

// partitionPivots returns pivot items which partitions items in the store
// into nsplits parititons.
func (m *Nitro) partitionPivots(snap *Snapshot, nsplits int) []*Item {
	var pivotItems []*Item

	tmpIter := m.NewIterator(snap)
	if tmpIter == nil {
		panic("iterator cannot be nil")
	}
	defer tmpIter.Close()

	barrier := m.store.GetAccesBarrier()
	token := barrier.Acquire()
	defer barrier.Release(token)

	pivotItems = append(pivotItems, nil) // start item
	pivotPtrs := m.store.GetRangeSplitItems(nsplits)
	for _, itmPtr := range pivotPtrs {
		itm := m.ptrToItem(itmPtr)
		tmpIter.Seek(itm.Bytes())
		if tmpIter.Valid() {
			prevItm := pivotItems[len(pivotItems)-1]
			// Find bigger item than prev pivot
			if prevItm == nil || m.insCmp(unsafe.Pointer(itm), unsafe.Pointer(prevItm)) > 0 {
				pivotItems = append(pivotItems, itm)
			}
		}
	}
	pivotItems = append(pivotItems, nil) // end item

	return pivotItems
}
