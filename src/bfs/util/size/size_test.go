package size

import (
	_ "github.com/golang/glog"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSize_Bytes(t *testing.T) {
	size := Bytes(1024 * 1024 * 1024 * 1024 * 1024)
	require.Equal(t, float64(1024*1024*1024*1024), size.ToKilobytes())
	require.Equal(t, float64(1024*1024*1024), size.ToMegabytes())
	require.Equal(t, float64(1024*1024), size.ToGigabytes())
	require.Equal(t, float64(1024), size.ToTerabytes())
	require.Equal(t, float64(1), size.ToPetabytes())
	require.Equal(t, "1PB", size.String())
}

func TestSize_Kilobytes(t *testing.T) {
	size := Bytes(KB + KB/2)
	require.Equal(t, 1.5, size.ToKilobytes())
	require.Equal(t, "1.5KB", size.String())
}

func TestSize_Megabytes(t *testing.T) {
	size := Bytes(MB + MB/2)
	require.Equal(t, 1.5, size.ToMegabytes())
	require.Equal(t, "1.5MB", size.String())
}

func TestSize_Gigabytes(t *testing.T) {
	size := Bytes(GB + GB/2)
	require.Equal(t, 1.5, size.ToGigabytes())
	require.Equal(t, "1.5GB", size.String())
}

func TestSize_Terabytes(t *testing.T) {
	size := Bytes(TB + TB/2)
	require.Equal(t, 1.5, size.ToTerabytes())
	require.Equal(t, "1.5TB", size.String())
}

func TestSize_Petabytes(t *testing.T) {
	size := Bytes(PB + PB/2)
	require.Equal(t, 1.5, size.ToPetabytes())
	require.Equal(t, "1.5PB", size.String())
}
