open Tjr_btree
open Block
open Object_map
open Omap_state

type state = omap_state

let the_state = ref {
    free=0;
    omap_root=0;
    omap_cache=();
    omap_additional_object_roots=0;
    file_caches=(fun _ -> ());
    dir_caches=(fun _ -> ());
  }
