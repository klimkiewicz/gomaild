package common

import (
	"sort"
)

type UIDSlice []uint32

// Implement sort.Interface methods.
func (uids UIDSlice) Len() int {
	return len(uids)
}

func (uids UIDSlice) Less(i, j int) bool {
	return uids[i] < uids[j]
}

func (uids UIDSlice) Swap(i, j int) {
	uids[i], uids[j] = uids[j], uids[i]
}

func (uids UIDSlice) Sort() {
	sort.Sort(sort.Reverse(uids))
}

// Contains method assumes the uids to be sorted in descending order.
func (uids UIDSlice) Contains(uid uint32) bool {
	idx := sort.Search(len(uids), func(idx int) bool {
		return uids[idx] <= uid
	})
	return idx < len(uids) && uids[idx] == uid
}

// Return new UIDSlice that contains all UIDs from `uids` that don't appear in
// `other`.
func (uids UIDSlice) Diff(other UIDSlice) UIDSlice {
	diff := make(UIDSlice, 0)

	for _, uid := range uids {
		if !other.Contains(uid) {
			diff = append(diff, uid)
		}
	}

	return diff
}

type TagsSet map[string]struct{}

func TagsSetFromSlice(slice []string) TagsSet {
	set := make(TagsSet)

	for _, tag := range slice {
		set.Add(tag)
	}

	return set
}

func (s TagsSet) Add(tag string) {
	s[tag] = struct{}{}
}

func (s TagsSet) Remove(tag string) {
	delete(s, tag)
}

func (s TagsSet) Set(tag string, value bool) {
	if value {
		s[tag] = struct{}{}
	} else {
		delete(s, tag)
	}
}

func (s TagsSet) Contains(tag string) bool {
	_, ok := s[tag]
	return ok
}

func (s TagsSet) Compare(other TagsSet) (added, removed TagsSet) {
	return
}

func (s TagsSet) Slice() []string {
	slice := make([]string, 0, len(s))
	for tag, _ := range s {
		slice = append(slice, tag)
	}
	return slice
}

func (s TagsSet) Update(others ...TagsSet) {
	for _, other := range others {
		for tag, _ := range other {
			s.Add(tag)
		}
	}
}
