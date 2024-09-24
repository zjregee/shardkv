package main

import (
	"hash/fnv"

	pb "github.com/zjregee/shardkv/proto"
)

type buffers map[uint64]*pb.Mutation

func newBuffers() *buffers {
	bs := make(buffers)
	return &bs
}

func (bs *buffers) addMutation(m *pb.Mutation) {
	key := encodeKey(m.Key)
	if _, ok := (*bs)[key]; !ok {
		(*bs)[encodeKey(m.Key)] = m
		return
	}
	switch (*bs)[key].Kind {
	case pb.MutationKind_Put:
		switch m.Kind {
		case pb.MutationKind_Put, pb.MutationKind_Delete:
			(*bs)[key] = m
		case pb.MutationKind_Append:
			(*bs)[key].Value = append((*bs)[key].Value, m.Value...)
		}
	case pb.MutationKind_Append:
		switch m.Kind {
		case pb.MutationKind_Put, pb.MutationKind_Delete:
			(*bs)[key] = m
		case pb.MutationKind_Append:
			(*bs)[key].Value = append((*bs)[key].Value, m.Value...)
		}
	case pb.MutationKind_Delete:
		switch m.Kind {
		case pb.MutationKind_Put, pb.MutationKind_Delete:
			(*bs)[key] = m
		case pb.MutationKind_Append:
			newM := &pb.Mutation{}
			newM.Kind = pb.MutationKind_Put
			newM.Key = m.Key
			newM.Value = m.Value
			(*bs)[key] = newM
		}
	}
}

func (bs *buffers) mutations() []*pb.Mutation {
	var ms []*pb.Mutation
	for _, m := range *bs {
		ms = append(ms, m)
	}
	return ms
}

func encodeKey(key []byte) uint64 {
	h := fnv.New64a()
	h.Write(key)
	return h.Sum64()
}
