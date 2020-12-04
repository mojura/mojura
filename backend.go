package dbl

// BackendInitializer will initialize a backend
type BackendInitializer interface {
	New(dbLocation string) (Backend, error)
}

// Backend represents a backend interface
type Backend interface {
	Transaction(func(BackendTransaction) error) error
	ReadTransaction(func(BackendTransaction) error) error
	Close() error
}

// BackendTransaction represents a backend transaction interface
type BackendTransaction interface {
	GetBucket(key []byte) BackendBucket
	GetOrCreateBucket(key []byte) (BackendBucket, error)
}

// BackendBucket represents a bucket
type BackendBucket interface {
	Get(key []byte) (value []byte)
	Put(key, value []byte) error
	Delete(key []byte) error
	Cursor() BucketCursor
	GetBucket(key []byte) BackendBucket
	GetOrCreateBucket(key []byte) (BackendBucket, error)
	ForEach(func(key, value []byte) error) error
}

// BucketCursor represents a bucket cursor
type BucketCursor interface {
	Seek(seekTo []byte) (key, value []byte)
	First() (key, value []byte)
	Next() (key, value []byte)
	Prev() (key, value []byte)
	Last() (key, value []byte)
}
