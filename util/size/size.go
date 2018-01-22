package size

import "fmt"

type Size float64

const (
	B  = 1
	KB = 1024
	MB = KB * 1024
	GB = MB * 1024
	TB = GB * 1024
	PB = TB * 1024
)

func Bytes(size float64) Size {
	return Size(size)
}

func Kilobytes(size float64) Size {
	return Size(size * KB)
}

func Megabytes(size float64) Size {
	return Size(size * MB)
}

func Gigabytes(size float64) Size {
	return Size(size * GB)
}

func Terabytes(size float64) Size {
	return Size(size * TB)
}

func Petabytes(size float64) Size {
	return Size(size * PB)
}

func (this Size) ToBytes() float64 {
	return float64(this)
}

func (this Size) ToKilobytes() float64 {
	return float64(this) / KB
}

func (this Size) ToMegabytes() float64 {
	return float64(this) / MB
}

func (this Size) ToGigabytes() float64 {
	return float64(this) / GB
}

func (this Size) ToTerabytes() float64 {
	return float64(this) / TB
}

func (this Size) ToPetabytes() float64 {
	return float64(this) / PB
}

func (this Size) String() string {
	switch {
	case float64(this) >= PB:
		return fmt.Sprint(this.ToPetabytes()) + "PB"
	case float64(this) >= TB:
		return fmt.Sprint(this.ToTerabytes()) + "TB"
	case float64(this) >= GB:
		return fmt.Sprint(this.ToGigabytes()) + "GB"
	case float64(this) >= MB:
		return fmt.Sprint(this.ToMegabytes()) + "MB"
	case float64(this) >= KB:
		return fmt.Sprint(this.ToKilobytes()) + "KB"
	default:
		return fmt.Sprint(float64(this)) + "B"
	}
}
