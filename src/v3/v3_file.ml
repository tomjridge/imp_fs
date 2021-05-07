(** Implement file data caching using Tjr_lib.Lru_with_slow_operations *)


(** We expect that a file descriptor will be supplied, along with the cache state.contents

Since we are writing direct to a file, we don't need to worry about alignment etc.

 *) 


