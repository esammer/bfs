package test

import (
	"os"
	"path/filepath"
)

type Directory struct {
	Name    string
	BaseDir string
	Path    string

	// True if we created the test directory.
	owner bool
}

func New(path ...string) *Directory {
	baseElems := len(path) - 1
	baseDir := filepath.Join(path[:baseElems]...)

	return &Directory{
		Name:    path[baseElems],
		BaseDir: baseDir,
		Path:    filepath.Join(baseDir, path[baseElems]),
	}
}

func (this *Directory) Create() error {
	if err := os.MkdirAll(this.Path, 0700); err != nil {
		return err
	}

	this.owner = true

	return nil
}

func (this *Directory) Destroy() error {
	if this.owner {
		if err := os.RemoveAll(this.Path); err != nil {
			return err
		}
	}

	return nil
}

func (this *Directory) String() string {
	return this.Path
}
