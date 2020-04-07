
(** Default uid/gid and perm for file creation *)
module S = struct
  type config = { uid:int; gid:int } [@@deriving yojson]
  let default_config = Some {
      uid=Unix.getuid();
      gid=Unix.getgid();
    }
  let filename="v1_config.json"
end

include Tjr_config.Make(S)

