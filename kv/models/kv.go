package models

import (
	"fmt"
	"sort"

	"github.com/pingcap-incubator/tinykv/kv/porcupine"
)

const (
	OpType_Get uint8 = iota
	OpType_Put
)

type KvInput struct {
	Op    uint8
	Key   string
	Value string
}

type KvOutput struct {
	Value string
}

var KvModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		m := make(map[string][]porcupine.Operation)
		for _, v := range history {
			key := v.Input.(KvInput).Key
			m[key] = append(m[key], v)
		}
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		ret := make([][]porcupine.Operation, 0, len(keys))
		for _, k := range keys {
			ret = append(ret, m[k])
		}
		return ret
	},
	Init: func() interface{} {
		// note: we are modeling a single key's value here;
		// we're partitioning by key, so this is okay
		return ""
	},
	Step: func(state, input, output interface{}) (bool, interface{}) {
		inp := input.(KvInput)
		out := output.(KvOutput)
		st := state.(string)
		if inp.Op == OpType_Get {
			// get
			return out.Value == st, state
		} else {
			// put
			return true, inp.Value
		}
	},
	DescribeOperation: func(input, output interface{}) string {
		inp := input.(KvInput)
		out := output.(KvOutput)
		switch inp.Op {
		case OpType_Get:
			return fmt.Sprintf("get('%s') -> '%s'", inp.Key, out.Value)
		case OpType_Put:
			return fmt.Sprintf("put('%s', '%s')", inp.Key, inp.Value)
		default:
			return "<invalid>"
		}
	},
}
