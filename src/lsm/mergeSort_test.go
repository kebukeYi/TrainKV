package lsm

import (
	"fmt"
	"testing"
)

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
		//{1, 2, 34, 77, 78, 99, 102, 123, 446, 556},
		{1, 2, 34, 77},
		//{2, 3, 4, 8, 78, 101, 122, 236, 666},
		{2, 3, 4, 8, 78},
		//{7, 32, 35, 68, 79, 135, 155, 736, 866},
		{7, 32}}

	mergedArray := MergeSortedArrays(arrays)
	fmt.Println("Merged array:", mergedArray)
}

func mergeSort(r []int) []int {
	length := len(r)
	if length <= 1 {
		return r
	}
	num := length / 2
	left := mergeSort(r[:num])
	right := mergeSort(r[num:])
	return merge(left, right)
}
func merge(left, right []int) (result []int) {
	l, r := 0, 0
	for l < len(left) && r < len(right) {
		if left[l] < right[r] {
			result = append(result, left[l])
			l++
		} else {
			result = append(result, right[r])
			r++
		}
	}
	result = append(result, left[l:]...)
	result = append(result, right[r:]...)
	return
}
func TestMereSort(t *testing.T) {
	arrays := []int{33, 22, 4444, 77, 8, 8, 12, 236, 1246, 6}
	mergedArray := mergeSort(arrays)
	fmt.Println("Merged array:", mergedArray)
}

func MergeSortNonR(array []int, n int) []int {
	temp := make([]int, n)
	gap := 1
	// 跨度: 1 2 4 8
	for gap < n {
		// 每个跨度下, 依次遍历各个区间, 每次当这个区间排完了之后就需要跳到下一个区间开始, 并且及时将当前排序后的区间结果 复制在 源数组中
		for i := 0; i < n; i += gap * 2 {
			index := i
			begin1 := i
			end1 := i + gap - 1
			begin2 := i + gap
			// 当下一组区间 不存在时, 剩余的元素不够一组, 跳出当前循环,进入下一轮,此时 gap 翻倍,之前的一组元素跟剩下的元素进行对比
			if begin2 >= n {
				break
			}
			end2 := i + gap*2 - 1
			if end2 >= n {
				end2 = n - 1
			}

			for (begin1 <= end1) && (begin2 <= end2) {
				if array[begin1] < array[begin2] {
					temp[index] = array[begin1]
					begin1++
				} else {
					temp[index] = array[begin2]
					begin2++
				}
				index++
			}
			for begin1 <= end1 {
				temp[index] = array[begin1]
				begin1++
				index++
			}
			for begin2 <= end2 {
				temp[index] = array[begin2]
				begin2++
				index++
			}
			fmt.Println(temp)
			copy(array[i:i+end2-i+1], temp[i:i+end2-i+1])
		}
		gap *= 2
	}
	return temp
}
func TestMergeSortNonR(t *testing.T) {
	//arrays := []int{33, 22, 4444, 77, 8, 8, 12, 236, 1246, 6}
	arrays := []int{33, 22, 9}
	//arrays := []int{33, 22}
	mergedArray := MergeSortNonR(arrays, len(arrays))
	fmt.Println("Merged array:", mergedArray)
}
