(** Collection of V1 modules *)

module V1_config = V1_config

(** Implementation of a file's data using a backing filesystem *)
module V1_file = V1_file

(** Generic filesystem implementation; independent of other V1 modules *)
module V1_generic = V1_generic

(** Not used *)
module NOT_USED_V1_resource_manager = V1_resource_manager

(** After V1_generic, this introduces some concrete types for V1_specific *)
module V1_types = V1_types

(** Instantiate generic implementation with V1-specific
   implementations. Stage_1(_).The_filesystem is the resulting
   filesystem *)
module V1_specific = V1_specific

