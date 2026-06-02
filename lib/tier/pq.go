package tier

// scoreHeap is a max-heap of entries by score, used to select the
// highest-priority bots when filling scarce hot slots (container/heap).
type scoreHeap []*entry

func (h scoreHeap) Len() int           { return len(h) }
func (h scoreHeap) Less(i, j int) bool { return h[i].score > h[j].score } // max-heap
func (h scoreHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *scoreHeap) Push(x any)        { *h = append(*h, x.(*entry)) }
func (h *scoreHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return e
}
