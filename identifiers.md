# Víðarr Identifiers
Rather than using incrementing identifiers, Víðarr identifies workflow
versions, workflow runs, and output files using SHA-256 hashes of key metadata.
The assumption is that if two objects have the same hash, they must be
equivalent. All workflow run matching is done by hash matching.

For all hashes, the hash is computed using a SHA-256 of the data described
below. All strings are converted to UTF-8 encoded bytes. Strings are not
permitted to contain the `NUL` (zero) byte. Some hashes contain the IDs of
other hashes. The hashes are encoded as ASCII strings in lowercase hexadecimal
representation. All JSON objects have keys in alphabetical order.

## Workflow Versions
A workflow version hash is present for each version of a workflow installed.
Even if the same WDL file is installed under two different names, there will be
two different workflow version hashes. It is computed as follows:

- _name_
- `NUL`
- _version_
- `NUL`
- `HEX_DIGITS(SHA256(` _WDL file UTF-8 bytes_ `))`
- `JSON(`_output parameters_`)`
- `JSON(`_input parameters_`)`
- for _filename_, _contents_ in _accessory files_; sorted by _filename_:
  - `NUL`
  - _filename_
  - `NUL`
  - `HEX_DIGITS(SHA256(`_contents_`))`

## Workflow Runs
Each workflow run has a hash consisting of data that is considered to uniquely
identify it but this does *not* include all information in a workflow run. That
is, there are *intentional* hash collisions for different workflow runs.

- _workflow-name_
- for _input_ in _input-ids_; sorted and unique
  - `NUL`
  - _hash_ from _input_ `=~ vidarr:`_server_`/`_hash_
- for _provider_, _identifier_ in _external-keys_; sorted by _provider_, then _identifier_:
  - `NUL`
  - `NUL`
  - _provider_
  - `NUL`
  - _identifier_
  - `NUL`
- for _name_, _value_ in _labels_; sorted by key:
  - `NUL`
  - _name_
  - `NUL`
  - `JSON(`_value_`)`
  - `NUL`

## Output Analysis: Files
The files provisioned out are given the ID:

- _workflow-run-identifier_
- `BASENAME(`_final output path_`)`

Note that if the provisioning output workflow renames files, that is now the hash.

## Output Analysis: URLs
The URLs provisioned out are given the ID:

- _workflow-run-identifier_
- _URL_

# Nulls in Hashes
The `NUL` characters are a kind of insurance against malicious names. Say the
hash was just did _name_ followed by _version_, then `foobar` + `1.0.0` becomes
indistinguishable from `foo` + `bar1.0.0`. Although no one would construct such
a name, but the nulls make it an easy way to prevent anyone from trying.
