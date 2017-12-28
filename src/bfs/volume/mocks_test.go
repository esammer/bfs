package volume

type MockFileSystem int
type MockReader int
type MockWriter int

func (*MockFileSystem) Open() error {
	return nil
}

func (*MockFileSystem) Close() error {
	return nil
}

func (*MockFileSystem) OpenRead(path string) (Reader, error) {
	return new(MockReader), nil
}

func (*MockFileSystem) OpenWrite(path string, blockSize int) (Writer, error) {
	return new(MockWriter), nil
}

func (*MockReader) Open() error {
	return nil
}

func (*MockReader) Close() error {
	return nil
}

func (*MockReader) Read(buffer []byte) (int, error) {
	return 0, nil
}

func (*MockWriter) Close() error {
	return nil
}

func (*MockWriter) Write(buffer []byte) (int, error) {
	return 0, nil
}
