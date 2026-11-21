// Package repository is the persistent pipe of the remote's pipeline (ADR-0019):
// the SQLite buffer that the collectors and the SDK write and the senders read
// (store-and-forward, ADR-0020; buffer limit, ADR-0009).
//
// It must not use requests or sender (depguard rule remote-repository): the
// buffer knows nothing about transport or delivery.
package repository
