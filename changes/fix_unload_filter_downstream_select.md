Recursive unloads now only apply when the upstream input file is from the same
Vidarr instance. (Previously, workflow run recursive unloads would also match
on input files with the same file ID from other Vidarr instances)
