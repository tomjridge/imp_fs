(** Interface level 1 with minifs *)

open V3_intf


module Make() = struct

  let new_tid = 
    let x = ref 0 in
    incr x;
    fun () -> 
      let y = !x in
      incr x;
      y

  open Tjr_monad.With_lwt

  let make ~(level1_ops:_ Level1_provides.ops) : _ Minifs_intf.Ops_type.ops =
    let o = level1_ops in
    let unlink pth   = o.unlink ~tid:(new_tid()) pth in
    let mkdir pth    = o.mkdir ~tid:(new_tid()) pth in
    let opendir pth  = o.opendir ~tid:(new_tid()) pth in
    (* let readdir dh   = o.readdir ~tid:(new_tid()) dh in *)
    let closedir dh  = o.closedir ~tid:(new_tid()) dh in
    let create pth   = o.create ~tid:(new_tid()) pth in
    let open_ pth    = o.open_ ~tid:(new_tid()) pth in
    let pread ~fd    = o.pread ~tid:(new_tid()) ~fd in
    let pwrite ~fd   = o.pwrite ~tid:(new_tid()) ~fd in
    let close fd     = o.close ~tid:(new_tid()) fd in
    let rename pth   = o.rename ~tid:(new_tid()) pth in
    let truncate pth = o.truncate ~tid:(new_tid()) pth in
    let stat pth     = o.stat ~tid:(new_tid()) pth in
    let symlink c    = o.symlink ~tid:(new_tid()) c in
    let readlink pth = o.readlink ~tid:(new_tid()) pth in
    let reset        = o.reset in
    (* FIXME need to standarise on a sensible interface for readdir *)
    let readdir dh = o.readdir ~tid:(new_tid()) dh >>= function
      | Error e -> return (Error e)
      | Ok xs -> 
        return (Ok (xs,{finished=(xs=[])}))
    in
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


