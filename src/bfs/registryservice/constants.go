package registryservice

const (
	// The default etcd key prefix. All keys will be prefixed with this value unless overridden.
	DefaultEtcdPrefix = "/bfs"
	// The etcd key prefix under which logical volumes are configured. This value is appended to the configured prefix.
	// This value is appended to the configured prefix or DefaultEtcdPrefix, otherwise.
	EtcdVolumesPrefix = "/volumes"
	// The etcd key prefix under which hosts are registered.
	// This value is appended to the configured prefix or DefaultEtcdPrefix, otherwise.
	EtcdHostsPrefix = "/hosts"
)
