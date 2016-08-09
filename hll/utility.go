package hll

func murmur3_64(data []byte, seed uint64) uint64 {
	var m uint64 = 0xc6a4a7935bd1e995
	var r uint64 = 47
	length := uint64(len(data))
	hashval := (seed & 0xffffffff) ^ (length * m)
	numeightbytes := length - (length & 7)
	for i := uint64(0); i < numeightbytes; i += 8 {
		k := uint64(data[i+7])
		k = k<<8 + uint64(data[i+6])
		k = k<<8 + uint64(data[i+5])
		k = k<<8 + uint64(data[i+4])
		k = k<<8 + uint64(data[i+3])
		k = k<<8 + uint64(data[i+2])
		k = k<<8 + uint64(data[i+1])
		k = k<<8 + uint64(data[i])
		k *= m
		k ^= k >> r
		k *= m
		hashval ^= k
		hashval *= m
	}
	remaining := length & 7
	if remaining > 0 {
		remaining_start := data[numeightbytes:]
		if remaining == 7 {
			hashval ^= uint64(remaining_start[6]) << 48
		}
		if remaining >= 6 {
			hashval ^= uint64(remaining_start[5]) << 40
		}
		if remaining >= 5 {
			hashval ^= uint64(remaining_start[4]) << 32
		}
		if remaining >= 4 {
			hashval ^= uint64(remaining_start[3]) << 24
		}
		if remaining >= 3 {
			hashval ^= uint64(remaining_start[2]) << 16
		}
		if remaining >= 2 {
			hashval ^= uint64(remaining_start[1]) << 8
		}
		hashval ^= uint64(remaining_start[0])
		hashval *= m
	}
	hashval ^= hashval >> r
	hashval *= m
	hashval ^= hashval >> r
	return hashval
}

func murmur3_32(data []byte, seed uint32) uint32 {
	var c1 uint32 = 0xcc9e2d51
	var c2 uint32 = 0x1b873593
	var r1 uint32 = 15
	var r2 uint32 = 13
	var m uint32 = 5
	var n uint32 = 0xe6546b64
	hashval := seed
	length := uint32(len(data))
	numfourbytes := length - (length & 3)
	for i := uint32(0); i < numfourbytes; i += 4 {
		k := uint32(data[i+3])
		k = k<<8 + uint32(data[i+2])
		k = k<<8 + uint32(data[i+1])
		k = k<<8 + uint32(data[i])
		k *= c1
		k = (k << r1) | (k >> (32 - r1))
		k *= c2
		hashval ^= k
		hashval = (hashval << r2) | (hashval >> (32 - r2))
		hashval = hashval*m + n
	}
	remaining := length & 3
	if remaining > 0 {
		remaining_start := data[numfourbytes:]
		var rem uint32 = 0
		if remaining == 3 {
			rem = uint32(remaining_start[2])
		}
		if remaining >= 2 {
			rem = rem<<8 + uint32(remaining_start[1])
		}
		rem = rem<<8 + uint32(remaining_start[0])
		rem *= c1
		rem = (rem << r1) | (rem >> (32 - r1))
		rem *= c2
		hashval ^= rem
	}
	hashval ^= length
	hashval = hashval ^ (hashval >> 16)
	hashval *= 0x85ebca6b
	hashval = hashval ^ (hashval >> 13)
	hashval *= 0xc2b2ae35
	hashval = hashval ^ (hashval >> 16)
	return hashval
}
