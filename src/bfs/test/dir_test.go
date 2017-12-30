package test

import (
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestDirectory(t *testing.T) {
	dir := New("build", "test", t.Name())
	require.NotNil(t, dir)
	require.Equal(t, filepath.Join("build", "test"), dir.BaseDir)
	require.Equal(t, t.Name(), dir.Name)
	require.Equal(t, filepath.Join("build", "test", t.Name()), dir.Path)

	err := dir.Create()
	require.NoError(t, err)

	info, err := os.Stat(dir.Path)
	require.NoError(t, err)
	require.NotNil(t, info)
	require.True(t, info.IsDir())

	err = dir.Destroy()
	require.NoError(t, err)
}

func TestDirectory_DirExists(t *testing.T) {
	dir := New("build", "test", t.Name())

	err := os.MkdirAll(dir.Path, 0700)
	require.NoError(t, err)

	err = dir.Create()
	require.NoError(t, err)

	err = dir.Destroy()
	require.NoError(t, err)
}

func TestDirectory_FileExists(t *testing.T) {
	dir := New("build", "test", t.Name())

	err := os.MkdirAll(dir.BaseDir, 0700)
	require.NoError(t, err)

	f, err := os.Create(dir.Path)
	require.NoError(t, err)
	err = f.Close()
	require.NoError(t, err)

	err = dir.Create()
	require.Error(t, err)

	if err, ok := err.(*os.PathError); ok {
		require.True(t, ok)
		require.Equal(t, dir.Path, err.Path)
		require.Equal(t, "mkdir", err.Op)
		require.Equal(t, syscall.ENOTDIR, err.Err)
	} else {
		require.Fail(t, "Error is of wrong type")
	}

	err = dir.Destroy()
	require.NoError(t, err)

	info, err := os.Stat(dir.Path)
	require.NoError(t, err)
	require.False(t, info.IsDir())

	err = os.RemoveAll(dir.Path)
	require.NoError(t, err)
}
