(** Interface level 1 with minifs *)

open Tjr_monad.With_lwt

open V3_intf

module Make() = struct

  let dont_log = !dont_log

  let live_tids = Hashtbl.create 10

  let new_tid = 
    let x = ref 0 in
    incr x;
    fun () -> 
      let y = !x in
      assert(dont_log || (Printf.printf "tid %d created\n%!" y; true));
      incr x;
      Hashtbl.replace live_tids y ();
      y

  let close_tid tid =
    Hashtbl.remove live_tids tid

  let rec debug_thread () = 
    (Lwt_unix.sleep 1.0 |>from_lwt) >>= fun () -> 
    Printf.printf "Live tids: %s\n%!" (String.concat "," (Hashtbl.to_seq_keys live_tids |> List.of_seq |> List.map string_of_int));
    debug_thread ()

  let _ = debug_thread ()

  open Tjr_monad.With_lwt

  module Msgs = Minifs_intf.Msgs

  let with_tid f = 
    let tid = new_tid () in
    f ~tid >>= fun r -> 
    close_tid tid;
    return r

  let make ~(level1_ops:_ Level1_provides.ops) : _ Level0_provides.ops =
    let o                                = level1_ops in
    let unlink pth                       = with_tid (o.unlink pth) in
    let mkdir pth                        = with_tid (o.mkdir pth) in
    let opendir pth                      = with_tid (o.opendir pth) in
    let closedir dh                      = with_tid (o.closedir dh) in
    let create pth                       = with_tid (o.create pth) in
    let open_ pth                        = with_tid (o.open_ pth) in
    let pread ~fd ~foff ~len ~buf ~boff  = with_tid (o.pread ~fd ~foff ~len ~buf ~boff) in
    let pwrite ~fd ~foff ~len ~buf ~boff = with_tid (o.pwrite ~fd ~foff ~len ~buf ~boff) in
    let close fd                         = with_tid (o.close    fd) in
    let rename p1 p2                     = with_tid (o.rename   p1 p2) in
    let truncate pth n                   = with_tid (o.truncate pth n) in
    let stat pth                         = with_tid (fun ~tid -> 
        assert(dont_log || begin
            Msgs.Stat pth |> Msgs.msg_from_client_to_yojson |> Yojson.Safe.to_string 
            |> fun s -> 
            Printf.printf "tid %d called %s\n%!" tid s;
            true end);
        o.stat ~tid pth)
    in
    let symlink c p  = with_tid (o.symlink c p) in
    let readlink pth = with_tid (o.readlink pth) in
    let reset        = o.reset in
    (* FIXME need to standarise on a sensible interface for readdir *)
    let readdir dh   = 
      with_tid (fun ~tid -> o.readdir ~tid dh >>= function
        | Error e -> return (Error e)
        | Ok xs -> 
          return (Ok (xs,{finished=(xs=[])})))
    in
    let (pread,pwrite) = convert_pread_pwrite_to_ba_buf ~pread ~pwrite in
    {
      root = o.root;
      unlink;
      mkdir;
      opendir;
      readdir;
      closedir;
      create;
      open_;
      pread;
      pwrite;
      close;
      rename;
      truncate;
      stat;
      symlink;
      readlink;
      reset;
    }
end


