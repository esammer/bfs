package client

const (
	// The default etcd key prefix. All keys will be prefixed with this value unless overridden.
	DefaultEtcdPrefix = "/bfs"
	// The etcd key prefix under which logical volumes are configured. This value is appended to the configured prefix.
	// This value is appended to the configured prefix or DefaultEtcdPrefix, otherwise.
	EtcdVolumesPrefix = "/volumes"
	// The etcd key prefix under which hosts are registered.
	// This value is appended to the configured prefix or DefaultEtcdPrefix, otherwise.
	EtcdHostsPrefix = "/hosts"
	// The etcd key prefix under which host configuration is kept. This value is appended to EtcdHostsPrefix.
	EtcdHostsConfigPrefix = "/config"
	// The etcd key prefix under which host status is kept. This value is appended to EtcdHostsPrefix.
	EtcdHostsStatusPrefix = "/status"
)
