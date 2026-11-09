// Package bufferschema embeds the schema.sql file in this local directory.
// Its import path starts with the module name from ../go.mod; it does not fetch
// SQL from a remote repository. The Go compiler reads schema.sql at build time.
package bufferschema

import _ "embed"

//go:embed schema.sql
var SQL string
