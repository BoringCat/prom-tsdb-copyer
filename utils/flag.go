package utils

import "gopkg.in/alecthomas/kingpin.v2"

type Argable interface {
	Arg(name string, help string) *kingpin.ArgClause
}
type Flagable interface {
	Flag(name string, help string) *kingpin.FlagClause
}
type KingPin interface {
	Argable
	Flagable
}
