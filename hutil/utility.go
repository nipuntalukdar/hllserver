package hutil

type Uint64Slice []uint64

func (u Uint64Slice) Len() int {
	return len(u)
}

func (u Uint64Slice) Less(i, j int) bool {
	return u[i] < u[j]
}

func (u Uint64Slice) Swap(i, j int) {
	u[i], u[j] = u[j], u[i]
}
