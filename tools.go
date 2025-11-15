package main

import (
	"time"

	"github.com/pkg/errors"
)

func Must[T any](val T, err error) T {
	if err != nil {
		panic(errors.WithStack(err).Error())
	}
	return val
}

func timeMin(a, b time.Time) time.Time {
	if a.After(b) {
		return b
	} else {
		return a
	}
}

func timeMax(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	} else {
		return b
	}
}
