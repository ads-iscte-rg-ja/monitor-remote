// Package sender is the consumer filter of the remote's pipeline (ADR-0019): it
// reads batches from the buffer (repository) and delivers them through the
// client (requests), with retries, quarantine and pause (ADR-0020, ADR-0011).
//
// It is the only package that knows both; requests and repository do not know
// each other (depguard rules remote-requests and remote-repository).
package sender
