package log

import (
	"errors"
	api "github.com/martin-lenweiter/proglog/api/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"io"
	"os"
	"strconv"
	"testing"
)

func TestLog(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T, log *Log,
	){
		"append and read a record succeeds": testAppendRead,
		"offset out of range error":         testOutOfRangeErr,
		"init with existing segments":       testInitExisting,
		"reader":                            testReader,
		"truncate":                          testTruncate,
	} {
		t.Run(scenario, func(t *testing.T) {
			dir, err := os.MkdirTemp("", "store-test")
			require.NoError(t, err)
			defer os.RemoveAll(dir)
			c := Config{}
			c.Segment.MaxStoreBytes = 32
			log, err := NewLog(dir, c)
			require.NoError(t, err)
			fn(t, log)
		})
	}
}

func testAppendRead(t *testing.T, log *Log) {
	appended := &api.Record{Value: []byte("hello world")}
	off, err := log.Append(appended)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)
	read, err := log.Read(off)
	require.NoError(t, err)
	require.Equal(t, read.Value, appended.Value)
}

func testOutOfRangeErr(t *testing.T, log *Log) {
	read, err := log.Read(1)
	require.Nil(t, read)
	var apiErr api.ErrOffsetOutOfRange
	errors.As(err, &apiErr)
	require.Equal(t, uint64(1), apiErr.Offset)
}

func testInitExisting(t *testing.T, log *Log) {
	var appended []*api.Record
	for i := 0; i < 3; i++ {
		appended = append(appended, &api.Record{Value: []byte(
			"hello world" + strconv.Itoa(i))})
	}
	for i := 0; i < 3; i++ {
		_, err := log.Append(appended[i])
		require.NoError(t, err)
	}
	err := log.Close()
	require.NoError(t, err)
	log, err = NewLog(log.Dir, log.Config)
	require.NoError(t, err)
	high, err := log.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), high)
	low, err := log.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), low)
	for i := 0; i < 3; i++ {
		rec, err := log.Read(uint64(i))
		require.NoError(t, err)
		require.Equal(t, rec.Value, appended[i].Value)
	}

}
func testReader(t *testing.T, log *Log) {
	appended := &api.Record{
		Value: []byte("hello world"),
	}
	off, err := log.Append(appended)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)
	reader := log.Reader()
	b, err := io.ReadAll(reader)
	require.NoError(t, err)
	read := &api.Record{}
	err = proto.Unmarshal(b[lenWidth:], read)
	require.NoError(t, err)
	require.Equal(t, appended.Value, read.Value)
}
func testTruncate(t *testing.T, log *Log) {
	append := &api.Record{
		Value: []byte("hello world"),
	}
	for i := 0; i < 3; i++ {
		_, err := log.Append(append)
		require.NoError(t, err)
	}
	err := log.Truncate(1)
	require.NoError(t, err)
	_, err = log.Read(0)
	require.Error(t, err)
}
