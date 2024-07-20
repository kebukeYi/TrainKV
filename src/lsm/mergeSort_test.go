package lsm

import (
	"fmt"
	"testing"
)

func mergeSortedArrays(arrays [][]int) []int {
	var result []int
	heap := PriorityQueue{}
	heap.Init()

	// 将每个数组的第一个元素放入堆中，并记录数组索引和元素索引
	for i, arr := range arrays {
		if len(arr) > 0 {
			heap.Push(Element{Value: arr[0], ArrayIndex: i, ElementIndex: 0})
		}
	}

	// 当堆不为空时，继续处理
	for !heap.IsEmpty() {
		// 从堆中取出最小的元素
		minElement := heap.Pop()
		result = append(result, minElement.Value)

		// 如果当前数组还有更多元素，则将下一个元素加入堆中
		if minElement.ElementIndex+1 < len(arrays[minElement.ArrayIndex]) {
			nextElement := Element{
				Value:        arrays[minElement.ArrayIndex][minElement.ElementIndex+1],
				ArrayIndex:   minElement.ArrayIndex,
				ElementIndex: minElement.ElementIndex + 1,
			}
			heap.Push(nextElement)
		}
	}

	return result
}

type Element struct {
	Value        int
	ArrayIndex   int
	ElementIndex int
}

type PriorityQueue struct {
	elements []Element
}

func (pq *PriorityQueue) Init() {
	pq.elements = make([]Element, 0)
}

func (pq *PriorityQueue) Push(e Element) {
	pq.elements = append(pq.elements, e)
	pq.heapifyUp(len(pq.elements) - 1)
}

func (pq *PriorityQueue) Pop() Element {
	lastElement := len(pq.elements) - 1
	element := pq.elements[0]
	pq.elements[0] = pq.elements[lastElement]
	pq.elements = pq.elements[:lastElement]
	pq.heapifyDown(0)
	return element
}

func (pq *PriorityQueue) IsEmpty() bool {
	return len(pq.elements) == 0
}

func (pq *PriorityQueue) heapifyUp(index int) {
	for index > 0 {
		parent := (index - 1) / 2
		if pq.elements[parent].Value > pq.elements[index].Value {
			pq.swap(parent, index)
			index = parent
		} else {
			break
		}
	}
}

func (pq *PriorityQueue) heapifyDown(index int) {
	size := len(pq.elements)
	for {
		leftChild := 2*index + 1
		rightChild := 2*index + 2
		smallest := index

		if leftChild < size && pq.elements[leftChild].Value < pq.elements[smallest].Value {
			smallest = leftChild
		}

		if rightChild < size && pq.elements[rightChild].Value < pq.elements[smallest].Value {
			smallest = rightChild
		}

		if smallest != index {
			pq.swap(index, smallest)
			index = smallest
		} else {
			break
		}
	}
}

func (pq *PriorityQueue) swap(i, j int) {
	pq.elements[i], pq.elements[j] = pq.elements[j], pq.elements[i]
}

func TestMergeHeaPSort(t *testing.T) {
	arrays := [][]int{
		{1, 2, 34, 77, 78, 99, 102, 123, 446, 556},
		{2, 3, 4, 8, 78, 101, 122, 236, 666},
		{7, 32, 35, 68, 79, 135, 155, 736, 866},
	}

	mergedArray := mergeSortedArrays(arrays)
	fmt.Println("Merged array:", mergedArray)
}

func mergeTwoSortedArrays(a, b []int) []int {
	merged := make([]int, 0, len(a)+len(b))
	i, j := 0, 0

	for i < len(a) && j < len(b) {
		if a[i] < b[j] {
			merged = append(merged, a[i])
			i++
		} else {
			merged = append(merged, b[j])
			j++
		}
	}

	merged = append(merged, a[i:]...)
	merged = append(merged, b[j:]...)

	return merged
}

func MergeSortedArrays(arrays [][]int) []int {
	if len(arrays) == 0 {
		return []int{}
	}
	if len(arrays) == 1 {
		return arrays[0]
	}

	mid := len(arrays) / 2
	left := MergeSortedArrays(arrays[:mid])
	right := MergeSortedArrays(arrays[mid:])

	return mergeTwoSortedArrays(left, right)
}

func TestMergeRecursionSorted(t *testing.T) {
	arrays := [][]int{
		{1, 2, 34, 77, 78, 99, 102, 123, 446, 556},
		{2, 3, 4, 8, 78, 101, 122, 236, 666},
		{7, 32, 35, 68, 79, 135, 155, 736, 866},
	}

	mergedArray := MergeSortedArrays(arrays)
	fmt.Println("Merged array:", mergedArray)
}
