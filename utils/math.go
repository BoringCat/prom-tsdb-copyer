package utils

func Int64Min(s1, s2 int64) int64 {
	if s1 > s2 {
		return s2
	}
	return s1
}
func Int64Max(s1, s2 int64) int64 {
	if s1 >= s2 {
		return s1
	}
	return s2
}
