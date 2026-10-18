package synclet

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseInputState(t *testing.T) {
	users := StreamDescriptor{Name: "users", Namespace: "public"}
	orders := StreamDescriptor{Name: "orders"}

	t.Run("empty", func(t *testing.T) {
		for _, in := range [][]byte{nil, []byte(""), []byte(" \n"), []byte("null"), []byte("[]")} {
			st, err := ParseInputState(in)
			require.NoError(t, err, string(in))
			assert.Equal(t, StateType(""), st.Type, string(in))
			assert.Nil(t, st.Stream(users))
			assert.Nil(t, st.Shared)
			assert.Nil(t, st.Legacy)
		}
	})

	t.Run("stream entries", func(t *testing.T) {
		st, err := ParseInputState([]byte(`[
			{"type":"STREAM","stream":{"stream_descriptor":{"name":"users","namespace":"public"},"stream_state":{"cursor":5}}},
			{"type":"STREAM","stream":{"stream_descriptor":{"name":"orders"},"stream_state":{"cursor":9}}}
		]`))
		require.NoError(t, err)
		assert.Equal(t, StateTypeStream, st.Type)
		assert.Len(t, st.Streams, 2)
		assert.JSONEq(t, `{"cursor":5}`, string(st.Stream(users)))
		assert.JSONEq(t, `{"cursor":9}`, string(st.Stream(orders)))
		assert.Nil(t, st.Stream(StreamDescriptor{Name: "users"}), "namespace is part of the key")
		assert.Nil(t, st.Stream(StreamDescriptor{Name: "missing"}))
	})

	t.Run("global", func(t *testing.T) {
		st, err := ParseInputState([]byte(`[{"type":"GLOBAL","global":{"shared_state":{"binlog":"000001:4"},"stream_states":[
			{"stream_descriptor":{"name":"users","namespace":"public"},"stream_state":{"pk":10}},
			{"stream_descriptor":{"name":"orders"}}
		]}}]`))
		require.NoError(t, err)
		assert.Equal(t, StateTypeGlobal, st.Type)
		assert.JSONEq(t, `{"binlog":"000001:4"}`, string(st.Shared))
		assert.Len(t, st.Streams, 2)
		assert.JSONEq(t, `{"pk":10}`, string(st.Stream(users)))
		assert.Nil(t, st.Stream(orders))
	})

	t.Run("legacy entry", func(t *testing.T) {
		st, err := ParseInputState([]byte(`[{"type":"LEGACY","data":{"timestamp":42}}]`))
		require.NoError(t, err)
		assert.Equal(t, StateTypeLegacy, st.Type)
		assert.JSONEq(t, `{"timestamp":42}`, string(st.Legacy))
	})

	t.Run("bare object is legacy", func(t *testing.T) {
		st, err := ParseInputState([]byte(`{"timestamp":42}`))
		require.NoError(t, err)
		assert.Equal(t, StateTypeLegacy, st.Type)
		assert.JSONEq(t, `{"timestamp":42}`, string(st.Legacy))
		assert.Nil(t, st.Stream(users))
	})

	t.Run("lowercase type", func(t *testing.T) {
		st, err := ParseInputState([]byte(`[{"type":"stream","stream":{"stream_descriptor":{"name":"orders"},"stream_state":{"cursor":1}}}]`))
		require.NoError(t, err)
		assert.Equal(t, StateTypeStream, st.Type)
		assert.JSONEq(t, `{"cursor":1}`, string(st.Stream(orders)))
	})

	t.Run("invalid json", func(t *testing.T) {
		_, err := ParseInputState([]byte(`[{"type":`))
		require.Error(t, err)
	})

	t.Run("unknown type", func(t *testing.T) {
		_, err := ParseInputState([]byte(`[{"type":"PARTIAL"}]`))
		require.Error(t, err)
	})
}
